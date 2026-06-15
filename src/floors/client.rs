use crate::floors::models::{Backdrop, Collection, Model};
use crate::floors::db::{bulk_update_backdrops, bulk_update_models, fetch_all_collections, fetch_all_tg_ids, fetch_collections_without_models, get_all_backdrops_item, get_all_models_item, save_new_backdrops, save_new_collections, save_new_models, PriceUpdateBackdrop, PriceUpdateModel};
use crate::floors::payload::{BybitResponse, PortalsResponse, PortalsResponseMap, XGiftItem, XGiftResponse, XGiftResponseMap};
use anyhow::{Context, Result, bail};
use ferogram::tl::enums::payments::{StarGiftUpgradeAttributes, StarGifts};
use ferogram::tl::enums::{Document, StarGift, StarGiftAttribute};
use ferogram::tl::functions;
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use wreq::{Client, Proxy};
use wreq_util::Emulation;
use crate::config::AppConfig;

pub async fn run(pool: &PgPool, config: Arc<AppConfig>, viewer: ferogram::Client) -> Result<()> {
    sync_telegram_collections(pool, &config, viewer).await.context("Failed to sync telegram collections")?;
    tracing::info!("Sync collections end");

    update_floor_prices(pool, &config).await.context("Failed to update floor prices")?;
    tracing::info!("Update end!");

    Ok(())
}

async fn sync_telegram_collections(pool: &PgPool, config: &AppConfig, viewer: ferogram::Client) -> Result<()> {

    let collections = viewer.invoke(&functions::payments::GetStarGifts {
        hash: 0,
    }).await?;

    let existing_ids = fetch_all_tg_ids(pool).await.context("Failed to fetch existing TG IDs")?;

    let mut new_collections_for_save = Vec::new();
    match collections {
        StarGifts::NotModified => bail!("Star gifts not modified"),
        StarGifts::StarGifts(star_gifts) => {
            for star_gift in star_gifts.gifts {
                match star_gift {
                    StarGift::StarGift(star_gift) => {
                        if let (Some(title), Some(_resale)) = (star_gift.title, star_gift.availability_resale) {
                            if !existing_ids.contains(&star_gift.id) {
                                let short_title: String = title
                                    .to_lowercase()
                                    .chars()
                                    .filter(|char| char.is_alphanumeric())
                                    .collect();
                                let collection = Collection {
                                    id: 0,
                                    title,
                                    short_title,
                                    tg_id: star_gift.id,
                                    active: false,
                                };
                                new_collections_for_save.push(collection);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    save_new_collections(pool, new_collections_for_save).await.context("Failed to save new collections")?;

    let collections_without_models = fetch_collections_without_models(pool).await.context("Failed to fetch collections without models")?;

    for collection in collections_without_models {
        tracing::info!("Get attributes for {}", collection.title);
        let gift_attributes = viewer.invoke(
            &functions::payments::GetStarGiftUpgradeAttributes {gift_id: collection.tg_id}
        ).await?;

        let StarGiftUpgradeAttributes::StarGiftUpgradeAttributes(gift_attributes)
            = gift_attributes;

        let mut models_to_save = Vec::new();
        let mut backdrops_to_save = Vec::new();
        for attribute in gift_attributes.attributes {
            match attribute {
                StarGiftAttribute::Model(gift_model) => {
                    match gift_model.document {
                        Document::Empty(_) => {
                            tracing::warn!("Can't get document for gift_model {}", gift_model.name)
                        }
                        Document::Document(doc) => {
                            let model = Model {
                                name: gift_model.name,
                                collection_id: collection.id,
                                floor_price: config.floor_price_default,
                                sticker_id: doc.id,
                                crafted: gift_model.crafted,
                            };
                            models_to_save.push(model);
                        }
                    }
                }
                StarGiftAttribute::Backdrop(gift_backdrop) => {
                    let backdrop = Backdrop {
                        name: gift_backdrop.name,
                        collection_id: collection.id,
                        floor_price: config.floor_price_default,
                        tg_backdrop_id: gift_backdrop.backdrop_id,
                    };
                    backdrops_to_save.push(backdrop);
                }
                _ => {}
            }
        }
        // println!("collection - {}", collection.title);
        // println!("models - {:#?}", models_to_save);
        // println!("backdrops - {:#?}", backdrops_to_save);

        save_new_models(pool, models_to_save).await?;

        save_new_backdrops(pool, backdrops_to_save).await?;

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    Ok(())
}

async fn update_floor_prices(pool: &PgPool, config: &AppConfig) -> Result<()> {
    let mut builder = Client::builder()
        .emulation(Emulation::Chrome144)
        .cookie_store(true)
        .timeout(Duration::from_secs(10));

    if let Some(proxy_arc) = config.current_wreq_proxy.load_full() {
        let proxy_str = proxy_arc.as_ref();
        builder = builder.proxy(Proxy::all(proxy_str)?);
    }

    let client = builder.build()?;

    let collections = fetch_all_collections(pool).await.context("Failed to fetch all collections")?;

    let short_titles = collections.iter()
        .map(|c| c.short_title.as_str())
        .collect::<Vec<&str>>()
        .join(",");
    let portal_response = get_portals_collections(&client, short_titles).await.context("Failed to get portals collections")?;

    let ton_price_in_dollars = get_ton_price(&client).await.context("Failed to get TON price")?;
    // just multiplay on floor_in_ton (0.015 - price per star)
    let stars_rate = ton_price_in_dollars / config.star_price_per_dollar;

    let models = get_all_models_item(&pool).await?;
    let backdrops = get_all_backdrops_item(&pool).await?;

    let mut models_to_update = Vec::new();
    let mut backdrops_to_update = Vec::new();

    for collection in collections {
        let collection_portals
            = portal_response.collections.get(&collection.short_title);
        let x_gift_data
            = get_x_gift_collection(&client, &collection.short_title).await.ok();

        if collection_portals.is_none() && x_gift_data.is_none() {
            tracing::error!("There is no data in either Portals or xGift for {}", collection.short_title);
            continue;
        }

        let unique_models = models.keys()
            .filter_map(|(col, name)| {
                (col == &collection.short_title).then_some(name)
            })
            .collect::<HashSet<_>>();

        let unique_backdrops = backdrops.keys()
            .filter_map(|(col, name)| {
                (col == &collection.short_title).then_some(name)
            })
            .collect::<HashSet<_>>();

        for model_name in unique_models {
            let p_floor = collection_portals.and_then(|p| {
                p.models.get(&*model_name).copied()
            });
            let x_floor = x_gift_data.as_ref().and_then(|x| {
                x.models.get(&*model_name).map(|m| calculate_x_floor(m, config))
            });

            let search_key = (collection.short_title.clone(), model_name.clone());
            if let Some(model_db) = models.get(&search_key) {
                if let Some(final_floor)
                    = calculate_final_floor(config,
                                            p_floor,
                                            x_floor,
                                            model_db.floor_price, 
                                            stars_rate) {
                    if final_floor != model_db.floor_price {
                        models_to_update.push(PriceUpdateModel {
                            id: model_db.id,
                            new_floor: final_floor
                        });
                    }
                }
            }
        }

        for backdrop_name in unique_backdrops {
            // let p_floor = collection_portals.and_then(|p| {
            //     p.backdrops.get(&*backdrop_name).copied()
            // });
            let x_floor = x_gift_data.as_ref().and_then(|x| {
                x.backdrops.get(&*backdrop_name).map(|b| calculate_x_floor(b, config) * config.backdrop_x_gift_reduction_factor)
            });

            let search_key = (collection.short_title.clone(), backdrop_name.clone());
            if let Some(backdrop_db) = backdrops.get(&search_key) {
                if let Some(final_floor)
                    = calculate_final_floor(config,
                                            // p_floor,
                                            None,
                                            x_floor,
                                            backdrop_db.floor_price,
                                            stars_rate) {
                    if final_floor != backdrop_db.floor_price {
                        backdrops_to_update.push(PriceUpdateBackdrop {
                            id: backdrop_db.id,
                            new_floor: final_floor
                        });
                    }
                }
            }
        }
    }

    bulk_update_models(&pool, models_to_update).await?;
    bulk_update_backdrops(&pool, backdrops_to_update).await?;

    Ok(())
}

fn calculate_x_floor(item: &XGiftItem, config: &AppConfig) -> f32 {
    let mut min_price = item.floor_price_ton.min(item.avg30d_price);

    if item.deals30d_count < config.min_reliable_deals {
        min_price = min_price.min(item.avg60d_price);
    }

    min_price
}

fn calculate_final_floor(
    config: &AppConfig,
    p_floor: Option<f32>,
    x_floor: Option<f32>,
    db_floor: i32,
    stars_rate: f32
) -> Option<i32> {
    let api_floor = match (p_floor, x_floor) {
        (Some(p), Some(x)) => p.min(x),
        (Some(p), None) => p,
        (None, Some(x)) => x,
        (None, None) => return None
    };

    let api_floor_stars = api_floor * stars_rate;

    let final_floor = if db_floor == config.floor_price_default {
        api_floor_stars
    } else {
        (db_floor as f32) * config.historical_price_weight
            + api_floor_stars * config.new_api_price_weight
    };

    Some(final_floor.max(config.floor_price_for_calc).round() as i32)
}

async fn get_portals_collections(client: &Client, short_titles: String)
    -> Result<PortalsResponseMap> {
    let mut portals_url
        = "https://portal-market.com/api/collections/filters?short_names=".to_string();
    portals_url.push_str(&*short_titles);
    println!("{portals_url}");
    for _ in 0..5 {
        if let Ok(response) = client.get(&portals_url).timeout(Duration::from_secs(30)).send().await {
            if let Ok(portals_response) = response.json::<PortalsResponse>().await {
                return Ok(portals_response.into_map());
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    tracing::error!("Can't get response from portals");
    bail!("Can't get response from portals")
}

async fn get_x_gift_collection(client: &Client, short_title: &str)
    -> Result<XGiftResponseMap> {
    let mut x_gift_url = "https://bot.xgift.tg/gifts/filters/".to_string();
    x_gift_url.push_str(&short_title);
    x_gift_url.push_str("?collectionType=upgradable");

    for _ in 0..2 {
        let response = client.get(&x_gift_url)
            .timeout(Duration::from_secs(10)).send().await;
        match response {
            Ok(response) => {
                if let Ok(x_gift_response) = response.json::<XGiftResponse>().await {
                    let x_gift_response_map = x_gift_response.into_map();
                    return Ok(x_gift_response_map)
                }
            }
            Err(err) => {
                tracing::warn!("Error for {short_title}, {}", err)
            }
        }
    }
    tracing::warn!("Can't get response from xGift for {short_title}");
    bail!("Can't get response from xGift for {short_title}")
}

async fn get_ton_price(client: &Client) -> Result<f32> {
    let url = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=TONUSDT";

    if let Ok(response) = client.get(url).send().await {
        if let Ok(data) = response.json::<BybitResponse>().await {
            if let Some(ticker) = data.result.list.first() {
                if let Ok(price) = ticker.last_price.parse::<f32>() {
                    return Ok(price)
                }
            }
        }
    }
    bail!("Can't get ton price")
}
