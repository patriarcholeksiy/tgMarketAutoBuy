pub mod floors;
pub mod accounts;
pub mod sniper;
mod config;

use crate::accounts::init_bot;
use crate::accounts::pool::AccountPool;
use crate::accounts::requests::build_targets_with_combos;
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::sniper::start_runner;
use anyhow::{Context, Result};
use sqlx::migrate;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tracing_appender::rolling::{Builder, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {

    let _log_guard = init_tracing();

    let config = Arc::new(AppConfig::load());

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await?;

    migrate!("./migrations").run(&pool).await.context("Failed to run migraions")?;

    let cache = Arc::new(Cache::init(&pool).await.context("Failed to init cache")?);

    let bot_pool = pool.clone();
    let bot_cache = Arc::clone(&cache);
    let bot_config = Arc::clone(&config);
    let mut bot_handle = tokio::spawn(async move {
        if let Err(e) = init_bot(&bot_pool, bot_cache, &bot_config).await {
            tracing::error!("Bot admin error: {e:#}");
            return Err(e);
        }
        Ok::<(), anyhow::Error>(())
    });

    let acc_pool = AccountPool::init(&pool, &config).await.context("Failed init account pool")?;

    let requests = build_targets_with_combos(&pool)
        .await
        .context("Failed to build targets")?;

    let mut sniper_handle = tokio::spawn(async move {
        start_runner(acc_pool, cache, requests, config).await;
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down...");
        }
        bot_res = &mut bot_handle => {
            tracing::error!("CRITICAL: Bot task stopped unexpectedly!");
            anyhow::bail!("Bot task stopped: {:?}", bot_res);
        }
        sniper_res = &mut sniper_handle => {
            tracing::error!("CRITICAL: Sniper task stopped unexpectedly!");
            anyhow::bail!("Sniper task stopped: {:?}", sniper_res);
        }
    }

    sniper_handle.abort();
    bot_handle.abort();

    let _ = tokio::join!(sniper_handle, bot_handle);
    
    Ok(())
}

fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let file_appender = Builder::new()
        .rotation(Rotation::DAILY)
        .filename_prefix("server.log")
        .max_log_files(7)
        .build("./logs")
        .expect("Err init file logger");

    let (non_blocking_file, guard)
        = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .with_writer(non_blocking_file)
        .with_ansi(false);

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true);

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,ferogram=error"))
        )
        .with(file_layer)
        .with(stdout_layer)
        .init();

    guard
}