use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use ferogram::{Button, Client, InlineKeyboard, InputMessage, OverflowStrategy, ReplyKeyboard, Update};
use ferogram::tl::enums::{Message, Peer};
use sqlx::{PgPool};
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::floors::client::run;

pub async fn init(pg_pool: &PgPool, cache: Arc<Cache>, config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let (bot, _) = Client::builder()
        .api_id(config.default_api_id)
        .api_hash(&config.default_api_hash)
        .update_queue_capacity(10)
        .update_overflow_strategy(OverflowStrategy::DropOldest)
        .connect().await?;

    bot.bot_sign_in(&*config.bot_token).await?;

    let waiting_for_proxy = Arc::new(AtomicBool::new(false));

    let mut updates = bot.stream_updates();
    while let Some(update) = updates.next().await {
        let bot_clone = bot.clone();
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

                if message.message == "/start" || message.message == "start" {
                    waiting_for_proxy.store(false, Ordering::SeqCst);
                    let kb = ReplyKeyboard::new()
                        .row([
                            Button::text("Gifts and Floors"),
                            Button::text("Accounts"),
                        ])
                        .resize();

                    bot_clone.send_message(sender_id, InputMessage::text("choose").reply_markup(kb)).await?;
                } else if waiting_for_proxy.load(Ordering::SeqCst) {
                    if let Some(formatted_proxy) = format_proxy_string(message.message.trim()) {
                        config.current_wreq_proxy.store(Some(Arc::new(formatted_proxy.clone())));

                        waiting_for_proxy.store(false, Ordering::SeqCst);

                        let _ = bot.send_message(
                            config.admin_user_id,
                            format!("The proxy is installed and ready to work!\n\n Final view: {formatted_proxy}")
                        ).await;
                    } else {
                        let _ = bot.send_message(
                            config.admin_user_id,
                            "Invalid proxy format.\nExpected: `IP:PORT:LOGIN:PASS` or `http://...`"
                        ).await;
                    }
                } else if message.message == "Gifts and Floors" {
                    let inline_kb = InlineKeyboard::new()
                        .row([
                            Button::callback("Update floor cache", b"floor:cache"),
                            Button::callback("Update floor prices", b"floor:update"),
                        ])
                        .row([
                            Button::callback("On proxy for markets", b"floor:on_proxy"),
                            Button::callback("Off proxy for markets", b"floor:off_proxy")
                        ]);

                    bot_clone.send_message(
                        sender_id,
                        InputMessage::text("choose").reply_markup(inline_kb)
                    ).await?;
                }
            }
            Update::MessageEdited(_) => {}
            Update::MessageDeleted(_) => {}
            Update::CallbackQuery(cb) => {
                let data = cb.data().unwrap_or("");
                println!("data - {data}");
                match data {
                    "floor:cache" => {
                        tracing::info!("Received a callback to update the cache");

                        let cache_clone = Arc::clone(&cache);
                        let pool_clone = pg_pool.clone();

                        let _ = cb.answer().text("Updating cache...").send(&bot_clone).await;

                        let admin_id = config.admin_user_id;
                        tokio::spawn(async move {
                            update_cache(cache_clone, &pool_clone, admin_id, &bot_clone).await;
                        });
                    }
                    "floor:update" => {
                        tracing::info!("Received a callback to update the floors");

                        let cache_clone = Arc::clone(&cache);
                        let pool_clone = pg_pool.clone();
                        let config_clone = config.clone();

                        let _ = cb.answer().text("Updating floors...").send(&bot_clone).await;

                        tokio::spawn(async move {
                            let admin_id = config_clone.admin_user_id;
                            match run(&pool_clone, &config_clone).await {
                                Ok(_) => {
                                    let _ = bot_clone.send_message(admin_id, "Update floors ended").await;
                                    update_cache(cache_clone, &pool_clone, admin_id, &bot_clone).await;
                                }
                                Err(err) => {
                                    tracing::error!("Error running update: {:?}", err);
                                    let _ = bot_clone.send_message(admin_id, format!("Err when update floors - {err:#?})")).await;
                                }
                            }
                        });
                    }
                    "floor:on_proxy" => {
                        waiting_for_proxy.store(true, Ordering::SeqCst);
                        let _ = cb.answer().text("Send proxy to chat").send(&bot_clone).await;
                    }
                    "floor:off_proxy" => {
                        waiting_for_proxy.store(false, Ordering::SeqCst);
                        config.current_wreq_proxy.store(None);

                        let _ = cb.answer().text("Proxy for market off").send(&bot_clone).await;
                        // let _ = bot_clone.send_message(config.admin_user_id, "Proxy for market off");
                    }
                    _ => {  }
                }
            }
            Update::InlineQuery(_) => {}
            Update::InlineSend(_) => {}
            Update::UserStatus(_) => {}
            Update::UserTyping(_) => {}
            Update::ParticipantUpdate(_) => {}
            Update::JoinRequest(_) => {}
            Update::MessageReaction(_) => {}
            Update::PollVote(_) => {}
            Update::BotStopped(_) => {}
            Update::ShippingQuery(_) => {}
            Update::PreCheckoutQuery(_) => {}
            Update::ChatBoost(_) => {}
            Update::GuestChatQuery(_) => {}
            Update::Raw(_) => {},
            _ => { todo!() }
        }
    }

    Ok(())
}

async fn update_cache(cache: Arc<Cache>, pg_pool: &PgPool, chat_id: i64, bot: &Client) {
    match cache.update(pg_pool).await {
        Ok(_) => {
            tracing::info!("Floor cache updated");
            let _ = bot.send_message(chat_id, "Floor cache successfully updated").await;
        }
        Err(err) => {
            tracing::error!("Error updating the floor cache: {err:#?}");
            let _ = bot.send_message(chat_id, format!("Cache update Error - {err:#?}")).await;
        }
    }
}

fn format_proxy_string(raw_input: &str) -> Option<String> {
    let input = raw_input.trim();

    if input.starts_with("http://") || input.starts_with("https://") || input.starts_with("socks5://") {
        return Some(input.to_string())
    }

    let parts: Vec<&str> = input.split(':').collect();

    match parts.len() {
        4 => {
            let ip = parts[0];
            let port = parts[1];
            let user = parts[2];
            let pass = parts[3];
            Some(format!("http://{}:{}@{}:{}", user, pass, ip, port))
        }
        2 => {
            let ip = parts[0];
            let port = parts[1];
            Some(format!("http://{}:{}", ip, port))
        }
        _ => None,
    }
}
