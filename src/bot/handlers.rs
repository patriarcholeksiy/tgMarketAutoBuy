use crate::accounts::pool::AccountPool;
use crate::bot::{keyboards, utils};
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::floors::client::run;
use ferogram::tl::enums::{Message, Peer};
use ferogram::update::{CallbackQuery, IncomingMessage};
use ferogram::{Client, InputMessage};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::Instant;
use crate::accounts;

pub async fn handle_message(
    message: IncomingMessage,
    bot: Client,
    account_pool: Arc<AccountPool>,
    pg_pool: PgPool,
    config: Arc<AppConfig>,
    waiting_proxy: &mut bool,
) {
    let Message::Message(message) = message.raw else { return; };
    if message.out { return; }

    let Peer::User(user) = message.peer_id else { return };
    if user.user_id != config.admin_user_id {
        tracing::warn!("Access attempt from someone else's ID: {}", user.user_id);
        return;
    }

    let response_msg = match message.message.trim() {
        "/start" | "start" => {
            *waiting_proxy = false;
            let reply_kb = keyboards::start_reply_keyboard();
            InputMessage::text(get_stats_text(&pg_pool, &account_pool).await).reply_markup(reply_kb)
        }
        "Gifts and Floors" => {
            let inline_kb = keyboards::service_floors_keyboard();
            InputMessage::text(get_stats_text(&pg_pool, &account_pool).await).reply_markup(inline_kb)
        }
        "Accounts" => {
            let inline_kb = keyboards::service_accounts_keyboard();
            InputMessage::text(get_stats_text(&pg_pool, &account_pool).await).reply_markup(inline_kb)
        }
        proxy if *waiting_proxy => {
            if let Some(formatted_proxy) = utils::format_proxy_string(proxy.trim()) {
                config.current_wreq_proxy.store(Some(Arc::new(formatted_proxy.clone())));
                *waiting_proxy = false;

                InputMessage::text(format!(
                    "The proxy is installed and ready to work!\n\nFinal view: {formatted_proxy}"
                ))
            } else {
                InputMessage::text(
                    "Invalid proxy format.\nExpected: `IP:PORT:LOGIN:PASS` or `http://...`"
                )
            }
        }
        _ => InputMessage::text("Unknown command")
    };

    if let Err(e) = bot.send_message(config.admin_user_id, response_msg).await {
        tracing::warn!("Failed to send admin message: {e:?}")
    };
}

async fn get_stats_text(pg_pool: &PgPool, account_pool: &AccountPool) -> String {
    let db_count = accounts::count_active_viewers(pg_pool).await.unwrap_or(0);
    let prog_count = account_pool.viewers.load().len();

    format!("Active viewers in db: {db_count}\nActive viewers in program: {prog_count}")
}

pub async fn handle_callback(
    cb: CallbackQuery,
    bot: Client,
    cache: Arc<Cache>,
    account_pool: Arc<AccountPool>,
    pg_pool: PgPool,
    config: Arc<AppConfig>,
    waiting_proxy: &mut bool,
) {
    if cb.user_id != config.admin_user_id {
        tracing::warn!("Unauthorized callback attempt from ID: {}", cb.user_id);
        return;
    }

    match cb.data().unwrap_or("") {
        "floor:cache_update" => update_floor_cache(bot, cache, pg_pool, config).await,
        "floor:price_update" => update_floor_price(cb, bot, cache, account_pool, pg_pool, config).await,

        "floor:on_proxy" => on_proxy(&cb, &bot, waiting_proxy).await,
        "floor:off_proxy" => off_proxy(&cb, &bot, &config, waiting_proxy).await,

        "accounts:add_new_viewers" => add_new_viewers(
            &cb,
            bot.clone(),
            account_pool.clone(),
            pg_pool.clone(),
            config.clone()
        ).await,
        "accounts:ping_update_status" => ping_update_status(&cb, bot.clone(), account_pool.clone()).await,
        _ => {  }
    }
}

async fn update_floor_cache(
    bot: Client,
    cache: Arc<Cache>,
    pg_pool: PgPool,
    config: Arc<AppConfig>,
) {
    tracing::info!("Received a callback to update the cache");

    tokio::spawn(async move {
        let start = Instant::now();
        match cache.update(&pg_pool).await {
            Ok(_) => {
                let duration = start.elapsed();

                tracing::info!("Floor cache updated. Take: {duration:?}");

                if let Err(e) = bot.send_message(
                    config.admin_user_id, format!("Floor cache successfully updated. Take: {duration:?}")
                ).await {
                    tracing::warn!("Failed to send message about floor cache update - {e:?}")
                };
            }
            Err(err) => {
                tracing::warn!("Error updating the floor cache: {err:#?}");

                if let Err(e) = bot.send_message(
                    config.admin_user_id, format!("Cache update Error - {err:#?}")
                ).await {
                    tracing::warn!("Failed to send message about error floor cache update - {e:?}")
                };
            }
        }
    });
}

async fn update_floor_price(
    cb: CallbackQuery,
    bot: Client,
    cache: Arc<Cache>,
    account_pool: Arc<AccountPool>,
    pg_pool: PgPool,
    config: Arc<AppConfig>,
) {
    tracing::info!("Received a callback to update the floors");

    let start = Instant::now();

    let viewer_clone = {
        let viewers = account_pool.viewers.load();
        match viewers.last() {
            Some(viewer_data) => viewer_data.client.clone(),
            None => {
                if let Err(e) = cb.answer().text("Err: empty viewers").send(&bot).await {
                    tracing::warn!("Failed to answer callback empty viewers - {e:?}");
                }
                return
            }
        }
    };

    if let Err(e) = cb.answer().text("Updating floors...").send(&bot).await {
        tracing::warn!("Failed to answer callback floor:update - {e:?}")
    };

    tokio::spawn(async move {
        match run(&pg_pool, &config, viewer_clone).await {
            Ok(_) => {
                if let Err(e) = bot.send_message(
                    config.admin_user_id, format!("Update floors ended. Take: {:?}", start.elapsed())
                ).await {
                    tracing::warn!("Failed to send message about floors update ended - {e:?}")
                };
                update_floor_cache(bot.clone(), cache.clone(), pg_pool.clone(), config.clone()).await;
            }
            Err(err) => {
                tracing::warn!("Error running update: {:?}", err);
                if let Err(e) = bot.send_message(
                    config.admin_user_id, format!("Err when update floors - {err:#?})")
                ).await {
                    tracing::warn!("Failed to send message about error running update - {e:?}")
                };
            }
        }
    });
}

async fn on_proxy(cb: &CallbackQuery, bot: &Client, waiting_proxy: &mut bool) {
    *waiting_proxy = true;

    if let Err(e) = cb.answer().text("Send proxy to chat").send(bot).await {
        tracing::warn!("Failed to answer callback on_proxy - {e:?}")
    };
}

async fn off_proxy(cb: &CallbackQuery, bot: &Client, config: &AppConfig, waiting_proxy: &mut bool) {
    *waiting_proxy = false;
    config.current_wreq_proxy.store(None);

    if let Err(e) = cb.answer().text("Proxy for market off").send(bot).await {
        tracing::warn!("Failed to answer callback off_proxy - {e:?}")
    };
}

async fn add_new_viewers(
    cb: &CallbackQuery,
    bot: Client,
    account_pool: Arc<AccountPool>,
    pg_pool: PgPool,
    config: Arc<AppConfig>,
) {
    tracing::info!("Trying add new viewers from DB");

    if let Err(e) = cb.answer().text("Add new viewers from DB...").send(&bot).await {
        tracing::warn!("Failed to answer callback accounts:add_new_viewers - {e:?}")
    };

    tokio::spawn(async move {
        match account_pool.add_viewers_dynamic(&pg_pool, &config).await {
            Ok(report) => {
                let report_msg = utils::generate_report_message(report);

                tracing::info!("{report_msg}");

                if let Err(e) = bot.send_message(config.admin_user_id, report_msg).await {
                    tracing::warn!("Failed to send message about add new viewers ended - {e:?}")
                };
            },
            Err(err) => {
                tracing::warn!("Error add new viewers: {:?}", err);

                if let Err(e) = bot.send_message(
                    config.admin_user_id, format!("Err when add new viewers - {err:#?})")
                ).await {
                    tracing::warn!("Failed to send message about error running update - {e:?}")
                };
            }
        }
    });
}

async fn ping_update_status(cb: &CallbackQuery, bot: Client, account_pool: Arc<AccountPool>) {
    tracing::info!("Ping update status for clients");

    if let Err(e) = cb.answer().text("Ping update for clients...").send(&bot).await {
        tracing::warn!("Failed to send cb for accounts:ping_update_status: {e:?}")
    }

    tokio::spawn(async move { account_pool.ping_update_status().await });
}