use std::sync::Arc;
use std::time::Duration;
use ferogram::{Button, Client, InlineKeyboard, InputMessage, OverflowStrategy, ReplyKeyboard, Update};
use ferogram::tl::enums::{Message, Peer};
use sqlx::{PgPool};
use tokio::sync::mpsc::UnboundedReceiver;
use crate::accounts::pool::{AccountPool, ViewerData};
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::floors::client::run;

pub async fn init_bot(pg_pool: &PgPool,
                      cache: Arc<Cache>,
                      acc_pool: Arc<AccountPool>,
                      config: Arc<AppConfig>,
                      mut notify_rx: UnboundedReceiver<String>)
    -> anyhow::Result<()> {
    let (bot, _) = Client::builder()
        .api_id(config.default_api_id)
        .api_hash(&config.default_api_hash)
        .session_string("")
        .update_queue_capacity(10)
        .update_overflow_strategy(OverflowStrategy::DropOldest)
        .connect().await?;

    bot.bot_sign_in(&*config.bot_token).await?;
    tracing::info!("Admin bot sign in");

    let alert_bot = bot.clone();
    let admin_id = config.admin_user_id;

    tokio::spawn(async move {
        tracing::info!("Alert worker for admin bot started");
        while let Some(message_text) = notify_rx.recv().await {
            if let Err(e) = alert_bot.send_message(admin_id, message_text).await {
                tracing::warn!("Failed to send alert message: {e:?}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let mut waiting_for_proxy = false;

    let mut updates = bot.stream_updates();
    while let Some(update) = updates.next().await {
        match update {
            Update::NewMessage(message) => {
                let Message::Message(message) = message.raw else {
                    continue
                };
                
                let sender_id = match message.peer_id {
                    Peer::User(user) => user.user_id,
                    _ => continue
                };
                if sender_id != config.admin_user_id {
                    tracing::warn!("Access attempt from someone else's ID: {sender_id}");
                    continue
                }

                match message.message.trim() {
                    "/start" | "start" => {
                        waiting_for_proxy = false;
                        let kb = ReplyKeyboard::new()
                            .row([
                                Button::text("Gifts and Floors"),
                                Button::text("Accounts"),
                            ])
                            .resize();

                        if let Err(e) = bot.send_message(sender_id, InputMessage::text("choose").reply_markup(kb)).await {
                            tracing::warn!("Failed to send start start message - {e:?}")
                        };
                    }
                    "Gifts and Floors" => {
                        let inline_kb = InlineKeyboard::new()
                            .row([
                                Button::callback("Update floor cache", b"floor:cache"),
                                Button::callback("Update floor prices", b"floor:update"),
                            ])
                            .row([
                                Button::callback("On proxy for markets", b"floor:on_proxy"),
                                Button::callback("Off proxy for markets", b"floor:off_proxy")
                            ]);

                        if let Err(e) = bot.send_message(sender_id, InputMessage::text("choose").reply_markup(inline_kb)).await {
                            tracing::warn!("Failed to send Gifts and Floors menu message - {e:?}")
                        };
                    }
                    "Accounts" => {
                        let inline_kb = InlineKeyboard::new()
                            .row([
                                Button::callback("Add new viewers", b"accounts:add_new_viewers"),
                            ])
                            .row([
                                Button::callback("Ping update status", b"accounts:ping_update_status")
                            ]);

                        if let Err(e) = bot.send_message(sender_id, InputMessage::text("choose").reply_markup(inline_kb)).await {
                            tracing::warn!("Failed to send Accounts menu message - {e:?}")
                        };
                    }
                    proxy if waiting_for_proxy => {
                        if let Some(formatted_proxy) = format_proxy_string(proxy.trim()) {
                            config.current_wreq_proxy.store(Some(Arc::new(formatted_proxy.clone())));
                            waiting_for_proxy = false;

                            if let Err(e) = bot.send_message(
                                config.admin_user_id,
                                format!("The proxy is installed and ready to work!\n\n Final view: {formatted_proxy}")
                            ).await {
                                tracing::warn!("Failed to send message about proxy - {e:?}")
                            }
                        } else {
                            if let Err(e) = bot.send_message(
                                config.admin_user_id,
                                "Invalid proxy format.\nExpected: `IP:PORT:LOGIN:PASS` or `http://...`"
                            ).await {
                                tracing::warn!("Failed to send message about invalid proxy - {e:?}")
                            };
                        }
                    }
                    _ => { }
                }
            }
            Update::CallbackQuery(cb) => {
                if cb.user_id != config.admin_user_id {
                    tracing::warn!("Unauthorized callback attempt from ID: {}", cb.user_id);
                    continue;
                }

                let data = cb.data().unwrap_or("");
                match data {
                    "floor:cache" => {
                        tracing::info!("Received a callback to update the cache");

                        let cache_clone = Arc::clone(&cache);
                        let pool_clone = pg_pool.clone();

                        if let Err(e) = cb.answer().text("Updating cache...").send(&bot).await {
                            tracing::warn!("Failed to answer callback floor:cache - {e:?}")
                        };

                        let admin_id = config.admin_user_id;
                        let bot_clone = bot.clone();
                        tokio::spawn(async move {
                            update_cache(cache_clone, &pool_clone, admin_id, &bot_clone).await;
                        });
                    }
                    "floor:update" => {
                        tracing::info!("Received a callback to update the floors");

                        let pool_clone = pg_pool.clone();
                        let config_clone = Arc::clone(&config);
                        let viewer_clone = {
                            let viewers = acc_pool.viewers.load();
                            match viewers.last() {
                                Some(viewer_data) => viewer_data.0.clone(),
                                None => {
                                    if let Err(e) = cb.answer().text("Err: empty viewers").send(&bot).await {
                                        tracing::warn!("Failed to answer callback empty viewers - {e:?}");
                                    }
                                    continue
                                }
                            }
                        };

                        let cache_clone = Arc::clone(&cache);

                        if let Err(e) = cb.answer().text("Updating floors...").send(&bot).await {
                            tracing::warn!("Failed to answer callback floor:update - {e:?}")
                        };

                        let bot_clone = bot.clone();
                        tokio::spawn(async move {
                            let admin_id = config_clone.admin_user_id;
                            match run(&pool_clone, config_clone, viewer_clone).await {
                                Ok(_) => {
                                    if let Err(e) = bot_clone.send_message(admin_id, "Update floors ended").await {
                                        tracing::warn!("Failed to send message about floors update ended - {e:?}")
                                    };
                                    update_cache(cache_clone, &pool_clone, admin_id, &bot_clone).await;
                                }
                                Err(err) => {
                                    tracing::warn!("Error running update: {:?}", err);
                                    if let Err(e) = bot_clone.send_message(admin_id, format!("Err when update floors - {err:#?})")).await {
                                        tracing::warn!("Failed to send message about error running update - {e:?}")
                                    };
                                }
                            }
                        });
                    }
                    "floor:on_proxy" => {
                        waiting_for_proxy = true;
                        if let Err(e) = cb.answer().text("Send proxy to chat").send(&bot).await {
                            tracing::warn!("Failed to answer callback on_proxy - {e:?}")
                        };
                    }
                    "floor:off_proxy" => {
                        waiting_for_proxy = false;
                        config.current_wreq_proxy.store(None);

                        if let Err(e) = cb.answer().text("Proxy for market off").send(&bot).await {
                            tracing::warn!("Failed to answer callback off_proxy - {e:?}")
                        };
                    }
                    "accounts:add_new_viewers" => {
                        tracing::info!("Trying add new viewers from DB");

                        let pg_pool_clone = pg_pool.clone();
                        let acc_pool_clone = Arc::clone(&acc_pool);
                        let config_clone = Arc::clone(&config);

                        if let Err(e) = cb.answer().text("Add new viewers from DB...").send(&bot).await {
                            tracing::warn!("Failed to answer callback accounts:add_new_viewers - {e:?}")
                        };

                        let bot_clone = bot.clone();
                        tokio::spawn(async move {
                            let admin_id = config_clone.admin_user_id;
                            match acc_pool_clone.add_viewers_dynamic(&pg_pool_clone, &config_clone).await {
                                Ok(report) => {
                                    let mut msg = String::new();

                                    if report.added.is_empty() && report.failed.is_empty() {
                                        msg.push_str("ℹ️ There are no new accounts to add.");
                                    } else {
                                        if !report.added.is_empty() {
                                            msg.push_str(&format!("✅ Successfully added: {}\n", report.added.len()));

                                            for (id, phone) in report.added {
                                                msg.push_str(&format!("• ID {id} with phone {phone:?}\n"));
                                            }
                                        }

                                        if !report.failed.is_empty() {
                                            msg.push_str(&format!("❌ Adding error: {}\n", report.failed.len()));
                                            for (id, phone, reason) in report.failed {
                                                msg.push_str(&format!("• ID {id} with phone {phone:?}: {reason}\n"));
                                            }
                                        }
                                    }

                                    tracing::info!("{msg}");
                                    if let Err(e) = bot_clone.send_message(admin_id, msg).await {
                                        tracing::warn!("Failed to send message about add new viewers ended - {e:?}")
                                    };
                                },
                                Err(err) => {
                                    tracing::warn!("Error add new viewers: {:?}", err);
                                    if let Err(e) = bot_clone.send_message(admin_id, format!("Err when add new viewers - {err:#?})")).await {
                                        tracing::warn!("Failed to send message about error running update - {e:?}")
                                    };
                                }
                            }
                        });
                    }
                    "accounts:ping_update_status" => {
                        let acc_pool_clone = Arc::clone(&acc_pool);

                        if let Err(e) = cb.answer().text("Ping update for all accounts...").send(&bot).await {
                            tracing::warn!("Failed to send cb for accounts:ping_update_status: {e:?}")
                        }

                        tokio::spawn(async move {
                            acc_pool_clone.ping_update_status().await;
                        });
                    }
                    _ => {  }
                }
            }
            _ => {  }
        }
    }

    Ok(())
}

async fn update_cache(cache: Arc<Cache>, pg_pool: &PgPool, chat_id: i64, bot: &Client) {
    match cache.update(pg_pool).await {
        Ok(_) => {
            tracing::info!("Floor cache updated");
            if let Err(e) = bot.send_message(chat_id, "Floor cache successfully updated").await {
                tracing::warn!("Failed to send message about floor cache update - {e:?}")
            };
        }
        Err(err) => {
            tracing::warn!("Error updating the floor cache: {err:#?}");
            if let Err(e) = bot.send_message(chat_id, format!("Cache update Error - {err:#?}")).await {
                tracing::warn!("Failed to send message about error floor cache update - {e:?}")
            };
        }
    }
}

fn format_proxy_string(raw_input: &str) -> Option<String> {
    let input = raw_input.trim();

    if input.starts_with("http://") || input.starts_with("https://") || input.starts_with("socks5://") {
        return Some(input.to_string())
    }

    let parts: Vec<&str> = input.split(':').collect();

    match parts.as_slice() {
        [ip, port, user, pass] => Some(format!("http://{user}:{pass}@{ip}:{port}")),
        [ip, port] => Some(format!("http://{ip}:{port}")),
        _ => None,
    }
}
