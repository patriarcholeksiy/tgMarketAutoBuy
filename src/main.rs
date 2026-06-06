pub mod floors;

use dotenvy::dotenv;
use std::env;
use sqlx::migrate;
use sqlx::postgres::PgPoolOptions;
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::{Builder, Rotation};
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use crate::floors::client::run;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    dotenv().ok();

    let _log_guard = init_tracing();

    let db_url = env::var("DATABASE_URL").expect("DB url didn't found");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    migrate!("./migrations").run(&pool).await?;

    run(&pool).await.unwrap();
    
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