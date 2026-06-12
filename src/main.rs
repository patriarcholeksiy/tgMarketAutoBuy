pub mod floors;
pub mod accounts;
pub mod sniper;
mod config;

use crate::accounts::pool::AccountPool;
use crate::accounts::requests::build_targets_with_combos;
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::sniper::runner::start_runner;
use sqlx::migrate;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::{Builder, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{

    let _log_guard = init_tracing();

    let config = Arc::new(AppConfig::load());

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await?;

    migrate!("./migrations").run(&pool).await?;

    let cache = Arc::new(Cache::init(&pool).await?);
    let acc_pool = AccountPool::init(&pool, &config).await;

    let requests = build_targets_with_combos(&pool).await?;

    let sniper_handle = tokio::spawn(async move {
        start_runner(acc_pool, cache, requests, config).await;
    });

    tokio::signal::ctrl_c().await?;
    sniper_handle.abort();

    tokio::time::sleep(Duration::from_secs(2)).await;
    
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
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy()
        )
        .with(file_layer)
        .with(stdout_layer)
        .init();

    guard
}