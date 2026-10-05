use crate::accounts::pool::{AccountPool, ViewerData};
use crate::accounts::telegram::ParsedGift;
use crate::config::AppConfig;
use crate::floors::cache::Cache;
use crate::sniper::strategy::SniperStrategy;
use ferogram::tl::enums::payments::{PaymentForm, ResaleStarGifts};
use ferogram::tl::enums::Invoice::Invoice;
use ferogram::tl::enums::{InputInvoice, LabeledPrice};
use ferogram::tl::functions;
use ferogram::tl::functions::payments::GetResaleStarGifts;
use ferogram::tl::types::InputInvoiceStarGiftResale;
use ferogram::InvocationError;
use sqlx::PgPool;
use std::f64::consts::GOLDEN_RATIO;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Clone)]
pub struct Context {
    pub acc_pool: Arc<AccountPool>,
    pub cache: Arc<Cache>,
    pub requests: Arc<[Arc<[GetResaleStarGifts]>]>,
    pub config: Arc<AppConfig>,
    pub counter: Arc<AtomicUsize>,
    pub pg_pool: PgPool,
}

pub async fn start_runner(acc_pool: Arc<AccountPool>,
                          mut new_viewer_rx: UnboundedReceiver<ViewerData>,
                          cache: Arc<Cache>,
                          requests: Arc<[Arc<[GetResaleStarGifts]>]>,
                          config: Arc<AppConfig>,
                          pg_pool: PgPool) {

    let counter = Arc::new(AtomicUsize::new(0));

    let global_start_time = Instant::now();
    let spawned_count = Arc::new(AtomicUsize::new(0));

    let current_viewers = acc_pool.viewers.load();

    let context = Context {
        acc_pool,
        cache,
        requests,
        config,
        counter,
        pg_pool,
    };

    for viewer_data in current_viewers.iter() {
        let context_clone = context.clone();

        spawn_viewer_task(viewer_data.clone(),
                          context_clone,
                          global_start_time,
                          Arc::clone(&spawned_count))
    }

    while let Some(new_viewer_data) = new_viewer_rx.recv().await {
        let context_clone = context.clone();
        spawn_viewer_task(new_viewer_data.clone(),
                          context_clone,
                          global_start_time,
                          Arc::clone(&spawned_count))
    }
}

fn spawn_viewer_task(
    viewer_data: ViewerData,
    context: Context,
    global_start_time: Instant,
    spawned_count: Arc<AtomicUsize>,
) {
    let viewer = viewer_data.client;
    let account_id = viewer_data.id;
    let phone = viewer_data.phone;
    let requests_len = context.requests.len();
    let num = context.config.coeff_num;
    let den = context.config.coeff_den;
    let default_fallback_price = ((125 * num) / den) as i32;

    tokio::spawn(async move {
        let market_poll_us = context.config.market_poll_ms as u64 * 1000;

        let slot = spawned_count.fetch_add(1, Ordering::Relaxed);

        let shift_fraction = (slot as f64 * GOLDEN_RATIO).fract();

        let ideal_phase_us = (shift_fraction * market_poll_us as f64) as u64;

        let uptime_us = global_start_time.elapsed().as_micros() as u64;
        let current_phase = uptime_us % market_poll_us;
        let delay_to_ideal = (ideal_phase_us + market_poll_us - current_phase) % market_poll_us;

        if delay_to_ideal > 0 {
            tokio::time::sleep(Duration::from_micros(delay_to_ideal)).await;
        }

        let mut interval = tokio::time::interval(Duration::from_micros(market_poll_us));
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
                    match err {
                        e if e.flood_wait_seconds().is_some() => {
                            let seconds = e.flood_wait_seconds().unwrap();
                            tracing::warn!(
                                "Client {} with phone: {:?} get flood wait to {seconds} seconds",
                                account_id, phone
                            );
                            tokio::time::sleep(Duration::from_secs(seconds + 2)).await;
                            continue
                        }
                        InvocationError::Rpc(rpc_err)
                        if rpc_err.code == 401 || rpc_err.name == "FROZEN_METHOD_INVALID" || rpc_err.name == "AUTH_KEY_DUPLICATED" => {
                            tracing::error!(
                                    "CRITICAL: Client {account_id} with phone {phone:?} received fatal error {}: {}. Stopping task...",
                                    rpc_err.code, rpc_err.name
                            );
                            context.acc_pool.deactivate_account(&context.pg_pool, account_id, phone, rpc_err.code).await;
                            break
                        }
                        InvocationError::Rpc(rpc_err) => {
                            tracing::warn!(
                                "RPC error during invocation {}: {} for viewer {} with phone: {:?}",
                                rpc_err.code, rpc_err.name, account_id, phone
                            );
                            continue
                        }
                        _ => {
                            tracing::warn!(
                                "Cannot invoke request for viewer {} with phone: {:?}, err - {err:?}",
                                account_id, phone
                            );
                            continue
                        }
                    }
                },
            };

            let ResaleStarGifts::ResaleStarGifts(resale_star_gifts) = resale_star_gifts;

            if resale_star_gifts.gifts.is_empty() {
                continue;
            }

            for gift in resale_star_gifts.gifts {
                let parsed = match ParsedGift::try_from_tg(gift) {
                    Some(p) => p,
                    None => continue,
                };

                let floor_price = {
                    let map = context.cache.floor_prices.load();
                    *map.get(&(parsed.sticker_id, parsed.backdrop_id as u16))
                        .unwrap_or(&default_fallback_price)
                };

                if SniperStrategy::should_buy(floor_price, parsed.price_in_stars, num, den) {
                    tracing::info!("Find gift {} with price {}", parsed.slug, parsed.price_in_stars);

                    let gift_trace_id = index;
                    tracing::info!(
                        name = parsed.slug,
                        gift_trace_id,
                        account_id,
                        phone = ?phone,
                        gift_id = gift_combos[combo_index].gift_id,
                        combo_index,
                        sticker_id = parsed.sticker_id,
                        backdrop_id = parsed.backdrop_id,
                        price_in_stars = parsed.price_in_stars,
                        price_in_ton = parsed.price_in_ton,
                        floor_price,
                        "Gift matched sniper strategy"
                    );

                    let price_in_ton = parsed.price_in_ton;
                    let slug = parsed.slug;
                    let buyers = Arc::clone(&context.acc_pool.buyers);

                    if buyers.is_empty() {
                        tracing::error!("CRITICAL: No buyer accounts available to purchase {slug}!");
                        break;
                    }

                    tokio::spawn(async move {
                        let is_bought = Arc::new(AtomicBool::new(false));
                        let mut buy_set = tokio::task::JoinSet::new();

                        for (buyer_client, buyer_peer) in buyers.iter() {
                            let buyer_client = buyer_client.clone();
                            let invoice = InputInvoiceStarGiftResale {
                                ton: true,
                                slug: slug.clone(),
                                to_id: buyer_peer.clone(),
                            };
                            let api_invoice: InputInvoice = invoice.into();
                            let is_bought_c = Arc::clone(&is_bought);
                            let slug_c = slug.clone();

                            buy_set.spawn(async move {
                                if is_bought_c.load(Ordering::Relaxed) {
                                    return;
                                }

                                let get_form = functions::payments::GetPaymentForm {
                                    invoice: api_invoice.clone(),
                                    theme_params: None,
                                };

                                let form = match buyer_client.invoke(&get_form).await {
                                    Ok(PaymentForm::StarGift(form)) => form,
                                    Ok(_) => {
                                        tracing::warn!("Payment form is not StarGift for {slug_c}");
                                        return;
                                    }
                                    Err(e) => {
                                        tracing::warn!("Buyer GetPaymentForm failed for {slug_c}: {e:?}");
                                        return;
                                    }
                                };

                                if is_bought_c.load(Ordering::Relaxed) {
                                    return;
                                }

                                let Invoice(form_invoice) = form.invoice;
                                let Some(LabeledPrice::LabeledPrice(price)) = form_invoice.prices.first() else {
                                    tracing::warn!("Labeled price is empty for {slug_c}!");
                                    return;
                                };

                                tracing::info!("Buyer successfully obtained payment form {} for {slug_c}", form.form_id);

                                if price_in_ton + AppConfig::MAX_SLIPPAGE_NANO_TON < price.amount {
                                    tracing::warn!(
                                        "Price in payment_form changed for {slug_c}, {} < {}, currency {}",
                                        price_in_ton,
                                        price.amount,
                                        form_invoice.currency
                                    );
                                    return;
                                }

                                // Ensure only one buyer proceeds to SendStarsForm
                                if is_bought_c.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_err() {
                                    return;
                                }

                                tracing::info!("Buyer submitting SendStarsForm for {slug_c} with form_id {}", form.form_id);

                                let req = functions::payments::SendStarsForm {
                                    form_id: form.form_id,
                                    invoice: api_invoice,
                                };

                                match buyer_client.invoke(&req).await {
                                    Ok(result) => {
                                        tracing::info!("SUCCESS: Gift {slug_c} was successfully purchased! Result: {result:#?}");
                                    }
                                    Err(e) => {
                                        // Allow other tasks to try if payment failed
                                        is_bought_c.store(false, Ordering::Relaxed);
                                        tracing::warn!("SendStarsForm failed for {slug_c}: {e:?}");
                                    }
                                }
                            });
                        }

                        while let Some(res) = buy_set.join_next().await {
                            if let Err(e) = res {
                                tracing::debug!("Join error in buy task: {e:?}");
                            }
                        }
                    });

                    // Break after triggering buy for the matching gift in this response
                    break;
                }
            }
        }
    });
}
