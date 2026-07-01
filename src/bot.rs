mod handlers;
mod keyboards;
mod utils;

use crate::accounts::pool::AccountPool;
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use ferogram::{Client, OverflowStrategy, TransportKind, Update};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn init_bot(pg_pool: &PgPool,
                      cache: Arc<Cache>,
                      acc_pool: Arc<AccountPool>,
                      config: Arc<AppConfig>,
                      mut notify_rx: UnboundedReceiver<String>)
    -> anyhow::Result<()> {
    let config_bot = Client::builder()
        .api_id(config.default_api_id)
        .api_hash(&config.default_api_hash)
        .transport(TransportKind::Abridged)
        .session_string("")
        .update_queue_capacity(10)
        .update_overflow_strategy(OverflowStrategy::DropOldest)
        .build()?;

    let (bot, _) = Client::connect(config_bot).await?;

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
                handlers::handle_message(
                    message,
                    bot.clone(),
                    acc_pool.clone(),
                    pg_pool.clone(),
                    config.clone(),
                    &mut waiting_for_proxy
                ).await;
            }
            Update::CallbackQuery(cb) => {
                handlers::handle_callback(
                    cb,
                    bot.clone(),
                    cache.clone(),
                    acc_pool.clone(),
                    pg_pool.clone(),
                    config.clone(),
                    &mut waiting_for_proxy,
                ).await;
            }
            _ => {  }
        }
    }

    Ok(())
}