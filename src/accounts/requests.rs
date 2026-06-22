use crate::accounts::db::get_active_collection_stickers;
use anyhow::{Context, Result};
use ferogram::tl::enums::StarGiftAttributeId;
use ferogram::tl::functions::payments::GetResaleStarGifts;
use ferogram::tl::types::StarGiftAttributeIdModel;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn build_targets_with_combos(pg_pool: &PgPool)
    -> Result<Arc<[Arc<[GetResaleStarGifts]>]>> {
    let collection_stickers = get_active_collection_stickers(pg_pool)
        .await
        .context("Failed to get active collection stickers from DB")?;
        
    let mut all_targets = Vec::with_capacity(collection_stickers.len());
    let mut total_combinations = 0usize;

    for collection_sticker in collection_stickers {
        let size = collection_sticker.sticker_ids.len();
        let mut combos = Vec::new();

        if size == 0 {
            tracing::warn!("Gift id - {} DOESN'T HAVE STICKERS ID", collection_sticker.gift_id);
            combos.push(create_request(collection_sticker.gift_id, None));
        } else {
            let all_attrs = collection_sticker.sticker_ids.iter()
                .map(|&id| create_model_attr(id))
                .collect();
            combos.push(create_request(collection_sticker.gift_id, Some(all_attrs)));

            for skip in 0..size {
                let mut attrs = Vec::with_capacity(size - 1);
                for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                    if j != skip { attrs.push(create_model_attr(id)) }
                }
                combos.push(create_request(collection_sticker.gift_id, Some(attrs)));
            }

            for skip1 in 0..size {
                for skip2 in (skip1 + 1)..size {
                    let mut attrs = Vec::with_capacity(size - 2);
                    for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                        if j != skip1 && j != skip2 { attrs.push(create_model_attr(id)); }
                    }
                    combos.push(create_request(collection_sticker.gift_id, Some(attrs)));
                }
            }

            combos.push(create_request(collection_sticker.gift_id, None));
        }
        total_combinations += combos.len();
        all_targets.push(Arc::from(combos));
    }

    tracing::info!("{} GetResaleStarGifts have been calculated", total_combinations);

    Ok(Arc::from(all_targets))
}

#[inline(always)]
fn create_model_attr(sticker_id: i64) -> StarGiftAttributeId {
    StarGiftAttributeId::Model(StarGiftAttributeIdModel { document_id: sticker_id })
}

#[inline(always)]
fn create_request(gift_id: i64, attributes: Option<Vec<StarGiftAttributeId>>) -> GetResaleStarGifts {
    GetResaleStarGifts {
        sort_by_price: false,
        sort_by_num: false,
        for_craft: false,
        stars_only: false,
        attributes_hash: None,
        gift_id,
        attributes,
        offset: "0".to_string(),
        limit: 1,
    }
}