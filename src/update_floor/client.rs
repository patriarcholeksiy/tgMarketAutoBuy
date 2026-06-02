use crate::models::{Backdrop, Collection, Model};
use crate::update_floor::db::{bulk_update_backdrops, bulk_update_models, fetch_all_collections, fetch_all_short_titles, fetch_all_tg_ids, fetch_collections_without_models, get_all_backdrops_item, get_all_models_item, save_new_backdrops, save_new_collections, save_new_models, PriceUpdateBackdrop, PriceUpdateModel};
use crate::update_floor::payload::{BybitResponse, CollectionPortals, PortalsResponse, PortalsResponseMap, XGiftResponse, XGiftResponseMap};
use ferogram::tl::enums::payments::{StarGiftUpgradeAttributes, StarGifts};
use ferogram::tl::enums::{Document, StarGift, StarGiftAttribute};
use ferogram::tl::functions;
use ferogram::OverflowStrategy;
use sqlx::PgPool;
use std::error::Error;
use std::time::Duration;
use std::{env, thread};
use std::collections::HashSet;
use std::sync::Arc;
use serde::de::Unexpected::Option;
use sqlx::__rt::sleep;
use wreq::{Client, Proxy};
use wreq_util::Emulation;

pub async fn run(pool: &PgPool) -> Result<(), Box<dyn Error>> {
    let (viewer, _) = ferogram::Client::builder()
        .api_id(env::var("DEFAULT_API_ID").unwrap().parse().unwrap())
        .api_hash(env::var("DEFAULT_API_HASH").unwrap())
        .update_queue_capacity(0)
        .session_string(env::var("DEFAULT_SESSION_STRING").unwrap())
        .update_overflow_strategy(OverflowStrategy::DropNewest)
        .connect().await?;

    println!("1");

    let collections = viewer.invoke(&functions::payments::GetStarGifts {
        hash: 0,
    }).await?;
    println!("2");

    let existing_ids = fetch_all_tg_ids(pool).await?;
    // println!("{:?}", existing_ids);
    println!("3");

    let mut new_collections_for_save = Vec::new();
    match collections {
        StarGifts::NotModified => { return Err(Box::from("Collections unfounded")) }
        StarGifts::StarGifts(star_gifts) => {
            for star_gift in star_gifts.gifts {
                match star_gift {
                    StarGift::StarGift(star_gift) => {
                        // if star_gift.title.is_some() &&
                        //     (star_gift.upgrade_stars.is_some()
                        //         || star_gift.auction
                        //         || star_gift.availability_resale.is_some()) {
                        if star_gift.title.is_some() && star_gift.availability_resale.is_some() {
                            if !existing_ids.contains(&star_gift.id) {
                                let title = star_gift.title.expect("REASON");
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
    println!("4");

    save_new_collections(pool, new_collections_for_save).await?;
    println!("5");
    let collections_without_models = fetch_collections_without_models(pool).await?;
    println!("6");
    for collection in collections_without_models {
        println!("{:#?}", collection);
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
                            eprintln!("Can't get document for gift_model {}", gift_model.name)
                        }
                        Document::Document(doc) => {
                            let model = Model {
                                name: gift_model.name,
                                collection_id: collection.id,
                                floor_price: 125,
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
                        floor_price: 125,
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

        thread::sleep(Duration::from_secs(3));
    }
    println!("6");

    let client = Client::builder()
        .emulation(Emulation::Chrome144)
        .proxy(Proxy::all(env::var("PROXY").unwrap())?)
        .cookie_store(true)
        .timeout(Duration::from_secs(10))
        .build()?;
    println!("7");

    let collections = fetch_all_collections(pool).await?;

    let short_titles = collections.iter()
        .map(|c| c.short_title.as_str())
        .collect::<Vec<&str>>()
        .join(",");
    println!("8");
    let portal_response = get_portals_collections(&client, short_titles).await?;
    println!("9");
    let ton_price = get_ton_price(&client).await?;
    println!("10");
    let stars_rate = ton_price / 0.015; // just multiplay on floor_in_ton (0.015 - price per star)

    let models = get_all_models_item(&pool).await?;
    let backdrops = get_all_backdrops_item(&pool).await?;

    let mut models_to_update = Vec::new();
    let mut backdrops_to_update = Vec::new();
    println!("11");
    for collection in collections {
        println!("{}", collection.title);
        let collection_portals
            = portal_response.collections.get(&collection.short_title);
        let x_gift_data
            = get_x_gift_collection(&client, &collection.short_title).await.ok();

        if collection_portals.is_none() && x_gift_data.is_none() {
            println!("There is no data in either Portals or xGift for {}", collection.short_title);
            continue;
        }

        let mut unique_models = HashSet::with_capacity(64);
        let mut unique_backdrops = HashSet::with_capacity(80);

        if let Some(portals) = collection_portals {
            for name in portals.models.keys() {
                unique_models.insert(name.clone());
            }
            for name in portals.backdrops.keys() {
                unique_backdrops.insert(name.clone());
            }
        }
        if let Some(xgift) = &x_gift_data {
            for name in xgift.models.keys() {
                unique_models.insert(name.clone());
            }
            for name in xgift.backdrops.keys() {
                unique_backdrops.insert(name.clone());
            }
        }

        for model_name in unique_models {
            let p_floor = collection_portals.and_then(|p| {
                p.models.get(&model_name).copied()
            });
            let x_floor = x_gift_data.as_ref().and_then(|x| {
                x.models.get(&model_name).map(|m| {
                    if m.deals30d_count < 30 { return m.floor_price_ton.min(m.avg30d_price) }
                    // (m.avg30d_price + m.floor_price_ton) / 2.0
                    m.floor_price_ton.min(m.avg30d_price).min(m.avg60d_price).min(m.avg90d_price)
                })
            });

            let api_floor = match (p_floor, x_floor) {
                (Some(p), Some(x)) => Some(p.min(x)),
                (Some(p), None) => Some(p),
                (None, Some(x)) => Some(x),
                (None, None) => None
            };

            if let Some(mut api_floor) = api_floor {
                let search_key = (collection.short_title.clone(), model_name.clone());

                if let Some(model_db) = models.get(&search_key) {
                    api_floor = api_floor * stars_rate;
                    let final_floor = if model_db.floor_price == 125 {
                        api_floor
                    } else {
                        (model_db.floor_price as f32) * 0.7 + api_floor * 0.3
                    };

                    let final_floor = final_floor.max(125.0).round() as i32;
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
            let p_floor = collection_portals.and_then(|p| {
                p.backdrops.get(&backdrop_name).copied()
            });
            let x_floor = x_gift_data.as_ref().and_then(|x| {
                x.backdrops.get(&backdrop_name).map(|b| {
                    if b.deals30d_count < 40 { return b.floor_price_ton.min(b.avg30d_price) }
                    // (b.avg30d_price + b.floor_price_ton) / 2.0
                    b.floor_price_ton.min(b.avg30d_price).min(b.avg60d_price).min(b.avg90d_price)
                })
            });

            let api_floor = match (p_floor, x_floor) {
                (Some(p), Some(x)) => Some(p.min(x)),
                (Some(p), None) => Some(p),
                (None, Some(x)) => Some(x),
                (None, None) => None
            };

            if let Some(mut api_floor) = api_floor {
                let search_key = (collection.short_title.clone(), backdrop_name.clone());

                if let Some(backdrop_db) = backdrops.get(&search_key) {
                    api_floor = api_floor * stars_rate;
                    let final_floor = if backdrop_db.floor_price == 125 {
                        api_floor
                    } else {
                        (backdrop_db.floor_price as f32) * 0.7 + api_floor * 0.3
                    };

                    let final_floor = final_floor.max(125.0).round() as i32;
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

    println!("Update end!");

    Ok(())
}

async fn get_portals_collections(client: &Client, short_titles: String)
    -> Result<PortalsResponseMap, String> {
    let mut portals_url
        = "https://portal-market.com/api/collections/filters?short_names=".to_string();
    portals_url.push_str(&*short_titles);
    for _ in 0..5 {
        println!("okk");
        if let Ok(response) = client.get(&portals_url).send().await {
            if let Ok(portals_response) = response.json::<PortalsResponse>().await {
                return Ok(portals_response.into_map());
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
    Err("Can't get response from portals".into())

}

async fn get_x_gift_collection(client: &Client, short_title: &String)
    -> Result<XGiftResponseMap, String> {
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
                println!("Error for {short_title}, {}", err)
            }
        }
        // if let Ok(response) = client.get(&x_gift_url)
        //     .timeout(Duration::from_secs(10)).send().await {
        //     if let Ok(x_gift_response) = response.json::<XGiftResponse>().await {
        //         let x_gift_response_map = x_gift_response.into_map();
        //         return Ok(x_gift_response_map)
        //     }
        // }
    }

    Err(format!("Can't get response from xGift for {short_title}").into())
}

async fn get_ton_price(client: &Client) -> Result<f32, String> {
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
    Err("Can't get ton price".into())
}

