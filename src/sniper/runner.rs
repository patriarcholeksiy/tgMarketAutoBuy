use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use ferogram::{Client};
use ferogram::tl::enums::{InputPeer, LabeledPrice};
use ferogram::tl::enums::Invoice::Invoice;
use ferogram::tl::enums::payments::{PaymentForm, ResaleStarGifts};
use ferogram::tl::functions;
use ferogram::tl::functions::payments::GetResaleStarGifts;
use ferogram::tl::types::{InputInvoiceStarGiftResale};
use tokio::task::{JoinSet};
use crate::accounts::pool::AccountPool;
use crate::accounts::telegram::ParsedGift;
use crate::floors::cache::Cache;
use crate::sniper::strategy::SniperStrategy;

pub struct Context {
    pub buyers: Arc<[(Client, InputPeer)]>,
    pub cache: Arc<Cache>,
    pub requests: Arc<[Arc<[GetResaleStarGifts]>]>,
    pub counter: Arc<AtomicUsize>,
}

pub async fn start_runner(pool: AccountPool,
                          cache: Arc<Cache>,
                          requests: Arc<[Arc<[GetResaleStarGifts]>]>) {

    let counter = Arc::new(AtomicUsize::new(0));
    let requests_len = requests.len();
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
            requests: Arc::clone(&requests),
            counter: Arc::clone(&counter),
        };

        tokio::time::sleep(Duration::from_millis((1610 / len_v * i) as u64)).await;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1610));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                let index = context.counter.fetch_add(1, Ordering::Relaxed);

                interval.tick().await;

                let gift_combos = &context.requests[index % requests_len];
                let combo_index = (index / requests_len) % gift_combos.len();

                let invoke_result
                    = viewer.invoke(&gift_combos[combo_index]).await;

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

                    let price_in_ton = parsed.price_in_ton;

                    let mut set = JoinSet::new();

                    for (buyer, input_peer) in context.buyers.iter() {
                        let buyer_clone = buyer.clone();
                        let input_peer_clone = input_peer.clone();
                        let slug = parsed.slug.clone();

                        let invoice = InputInvoiceStarGiftResale {
                            ton: true,
                            slug,
                            to_id: input_peer_clone,
                        };

                        let get_payment_form = Arc::new(functions::payments::GetPaymentForm {
                            invoice: invoice.clone().into(),
                            theme_params: None,
                        });

                        set.spawn(tokio::spawn(async move {

                            let mut payment_set = JoinSet::new();

                            for _ in 0..10 {
                                let buyer_clone_form = buyer_clone.clone();
                                let get_payment_form_clone = Arc::clone(&get_payment_form);
                                payment_set.spawn(async move {
                                    buyer_clone_form.invoke(get_payment_form_clone.as_ref()).await
                                });
                            }

                            let mut fastest_payment_form = None;

                            while let Some(join_result) = payment_set.join_next().await {
                                match join_result {
                                    Ok(invoke_result) => {
                                        match invoke_result {
                                            Ok(join_payment_form) => {
                                                fastest_payment_form = Some(join_payment_form);
                                                payment_set.abort_all();
                                                break;
                                            }
                                            Err(err) => {
                                                if let Some(sec) = err.flood_wait_seconds() {
                                                    tracing::warn!("Buyer get flood wait - {sec} sec")
                                                } else {
                                                    tracing::warn!("Buyer get err - {err:?}")
                                                }
                                            }
                                        }
                                    }
                                    Err(join_err) => {
                                        tracing::warn!("The join_thread panicked or was canceled: {join_err:?}");
                                    }
                                }
                            }

                            let Some(payment_form) = fastest_payment_form else {
                                tracing::warn!("Client can't get payment form");
                                return
                            };

                            let PaymentForm::StarGift(form_star_gift) = payment_form else {
                                tracing::warn!("The payment form is not StarGift");
                                return
                            };

                            let Invoice(form_invoice) = form_star_gift.invoice;

                            let Some(LabeledPrice::LabeledPrice(price)) = &form_invoice.prices.first() else {
                                tracing::warn!("Labeled price is empty!");
                                return
                            };

                            // 150_000_000 its 0.15 ton it is necessary if the price is sensitive
                            if price_in_ton + 150_000_000 < price.amount {
                                tracing::warn!(
                                    "Price in payment_form is changed, {} < {}, with currency {}",
                                    price_in_ton,
                                    price.amount,
                                    form_invoice.currency
                                );
                                return
                            }

                            let buy_result = buyer_clone.invoke(&functions::payments::SendStarsForm {
                                form_id: form_star_gift.form_id,
                                invoice: invoice.into()
                            }).await;

                            match buy_result {
                                Ok(ok) =>
                                    tracing::info!("The gift was successfully purchased - {ok:?}"),
                                Err(err) =>
                                    tracing::warn!("The gift was not bought, err - {err:?}")
                            }
                        }));
                    }
                    set.join_all().await;
                }
            }
        });
    }
}