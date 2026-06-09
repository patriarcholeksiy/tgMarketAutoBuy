use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use ferogram::Client;
use ferogram::tl::enums::{Document, InputPeer, StarGift, StarGiftAttribute, StarsAmount};
use ferogram::tl::enums::payments::{ResaleStarGifts};
use ferogram::tl::functions;
use ferogram::tl::types::{InputInvoiceStarGiftResale};
use futures::future::join_all;
use crate::accounts::pool::AccountPool;
use crate::accounts::telegram::ParsedGift;
use crate::floors::cache::Cache;
use crate::sniper::strategy::SniperStrategy;

pub struct Context {
    pub buyers: Arc<[(Client, InputPeer)]>,
    pub cache: Arc<Cache>,
    pub target_gifts: Arc<Vec<i64>>,
    pub counter: Arc<AtomicUsize>,
}

pub async fn start_runner(pool: AccountPool, cache: Arc<Cache>, target_gifts: Arc<Vec<i64>>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let target_gifts_len = target_gifts.len();
    let len_v = pool.viewers.len();

    if pool.viewers.len() == 0 {
        tracing::error!("Doesn't have viewers for scan market!");
        panic!("len viewers = 0")
    }

    if pool.buyers.len() == 0 {
        tracing::error!("Doesn't have buyers for buy!");
        panic!("len buyers = 0")
    }

    for (i, viewer) in pool.viewers.into_iter().enumerate() {
        let context = Context {
            buyers: Arc::clone(&pool.buyers),
            cache: Arc::clone(&cache),
            target_gifts: Arc::clone(&target_gifts),
            counter: Arc::clone(&counter),
        };

        tokio::time::sleep(Duration::from_millis((1600 / len_v * i) as u64)).await;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1600));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                let index = context.counter.fetch_add(1, Ordering::Relaxed);

                interval.tick().await;

                let current_target = context.target_gifts[index % target_gifts_len];

                let invoke_result
                    = viewer.invoke(&functions::payments::GetResaleStarGifts {
                    sort_by_price: false,
                    sort_by_num: false,
                    for_craft: false,
                    stars_only: false,
                    attributes_hash: None,
                    gift_id: current_target,
                    attributes: None,
                    offset: "0".to_string(),
                    limit: 1
                }).await;

                let resale_star_gifts = match invoke_result {
                    Ok(res) => res,
                    Err(err) => {
                        if let Some(seconds) = err.flood_wait_seconds() {
                            tracing::warn!("Client get flood wait to {seconds} seconds");
                            tokio::time::sleep(Duration::from_secs(seconds + 2)).await;
                        } else {
                            tracing::warn!("Cannot invoke request, err - {err:?}");
                        }
                        continue
                    },
                };

                let ResaleStarGifts::ResaleStarGifts(resale_star_gifts) = resale_star_gifts;

                let first_gift = match resale_star_gifts.gifts.into_iter().next() {
                    Some(gift) => gift,
                    None => {
                        tracing::warn!("First gift is None");
                        continue
                    }
                };

                let parsed = match ParsedGift::try_from_tg(first_gift) {
                    Some(p) => p,
                    None => continue,
                };

                let floor_price = {
                    let map = context.cache.floor_prices.load();
                    *map.get(&(parsed.sticker_id, parsed.backdrop_id as u16)).unwrap_or(&250)
                };

                if SniperStrategy::should_buy(floor_price, parsed.price_in_stars) {
                    tracing::info!("Find gift {} with price {}", parsed.slug, parsed.price_in_stars);

                    let mut handlers = vec![];

                    for (buyer, input_peer) in context.buyers.iter() {
                        let buyer_clone = buyer.clone();
                        let input_peer_clone = input_peer.clone();
                        let slug = parsed.slug.clone();

                        handlers.push(tokio::spawn(async move {
                            let buy_result = buyer_clone.invoke(&functions::payments::SendStarsForm {
                                form_id: 0,
                                invoice: InputInvoiceStarGiftResale {
                                    ton: true,
                                    slug,
                                    to_id: input_peer_clone,
                                }.into()
                            }).await;

                            match buy_result {
                                Ok(ok) =>
                                    tracing::info!("The gift was successfully purchased"),
                                Err(err) =>
                                    tracing::warn!("The gift was not bought, err - {err}")
                            }
                        }));
                    }
                    join_all(handlers).await;
                }
            }
        });
    }
}