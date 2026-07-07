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

            // First pass: odd distances with even skip1 indices
            for distance in (1..size).step_by(2) {
                for skip1 in (0..size).step_by(2) {
                    let skip2 = skip1 + distance;
                    if skip2 < size {
                        let mut attrs = Vec::with_capacity(size - 2);
                        for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                            if j != skip1 && j != skip2 { attrs.push(create_model_attr(id)); }
                        }
                        combos.push(create_request(collection_sticker.gift_id, Some(attrs)));
                    }
                }
            }

            // Second pass: even distances with all skip1 indices
            for distance in (2..size).step_by(2) {
                for skip1 in 0..size {
                    let skip2 = skip1 + distance;
                    if skip2 < size {
                        let mut attrs = Vec::with_capacity(size - 2);
                        for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                            if j != skip1 && j != skip2 { attrs.push(create_model_attr(id)); }
                        }
                        combos.push(create_request(collection_sticker.gift_id, Some(attrs)));
                    }
                }
            }

            // Third pass: odd distances with odd skip1 indices
            for distance in (1..size).step_by(2) {
                for skip1 in (1..size).step_by(2) {
                    let skip2 = skip1 + distance;
                    if skip2 < size {
                        let mut attrs = Vec::with_capacity(size - 2);
                        for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                            if j != skip1 && j != skip2 { attrs.push(create_model_attr(id)); }
                        }
                        combos.push(create_request(collection_sticker.gift_id, Some(attrs)));
                    }
                }
            }

            // Generate three-element combinations, sorted by sum for optimal spacing
            if size >= 3 {
                let mut three_combos = Vec::new();

                for i in 0..size {
                    for j in (i+1)..size {
                        for k in (j+1)..size {
                            three_combos.push((i, j, k));
                        }
                    }
                }

                // Sort by sum of indices, then by indices themselves for better spacing
                three_combos.sort_by_key(|&(i, j, k)| (i + j + k, i, j));

                for (skip1, skip2, skip3) in three_combos {
                    let mut attrs = Vec::with_capacity(size - 3);
                    for (j, &id) in collection_sticker.sticker_ids.iter().enumerate() {
                        if j != skip1 && j != skip2 && j != skip3 {
                            attrs.push(create_model_attr(id));
                        }
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