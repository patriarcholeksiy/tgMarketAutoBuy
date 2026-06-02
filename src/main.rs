pub mod update_floor;
pub mod models;

use dotenvy::dotenv;
use std::env;
use sqlx::migrate;
use sqlx::postgres::PgPoolOptions;
use crate::update_floor::client::run;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    dotenv().ok();

    let db_url = env::var("DATABASE_URL").expect("DB url didn't found");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    migrate!("./migrations").run(&pool).await?;

    run(&pool).await.unwrap();

    println!("okkkk");
    Ok(())
}