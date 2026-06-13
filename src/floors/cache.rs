use std::sync::Arc;
use arc_swap::ArcSwap;
use rustc_hash::{FxHashMap};
use sqlx::PgPool;
use crate::floors::db::get_all_floor_active_collections;

pub struct Cache {
    pub floor_prices: ArcSwap<FxHashMap<(i64, u16), i32>>
}

impl Cache {
    pub async fn init(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let map = get_all_floor_active_collections(pool).await?;
        tracing::info!("Successfully loaded {} cache entries", map.len());
        Ok(Cache { floor_prices: ArcSwap::from_pointee(map) })
    }

    pub async fn update(&self, pool: &PgPool) -> Result<(), sqlx::Error>{
        match get_all_floor_active_collections(&pool).await {
            Ok(new_map) => self.floor_prices.store(Arc::new(new_map)),
            Err(err) => return Err(err)
        }

        Ok(())
    }
}