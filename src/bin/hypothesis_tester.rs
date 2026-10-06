use anyhow::{bail, Context, Result};
use ferogram::tl::enums::payments::{ResaleStarGifts, SavedStarGifts};
use ferogram::tl::enums::{Document, InputPeer, InputSavedStarGift, SavedStarGift, StarGift, StarGiftAttribute, StarGiftAttributeId, StarsAmount};
use ferogram::tl::functions::payments::{GetResaleStarGifts, GetSavedStarGifts, UpdateStarGiftPrice};
use ferogram::tl::types::{InputSavedStarGiftSlug, StarGiftAttributeIdModel, StarsAmount as StarsAmountData};
use ferogram::{Client, InvocationError, NoRetries, OverflowStrategy, TransportKind};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tg_market_autobuy::accounts::db::{get_account_by_id, get_active_accounts, get_collection_stickers_by_gift_id, Account, AccountRole};
use tg_market_autobuy::config::AppConfig;
use tokio::time::sleep;

#[derive(Clone)]
struct TestGiftInfo {
    slug: String,
    title: String,
    num: i32,
    gift_id: i64,
    sticker_id: Option<i64>,
    currently_listed: bool,
    listed_price: Option<i64>,
}

struct HypothesisDef {
    id: usize,
    name: &'static str,
    description: &'static str,
    generator: fn(gift_id: i64, all_stickers: &[i64], target_sticker: Option<i64>) -> Vec<GetResaleStarGifts>,
}

#[derive(Debug, Clone)]
struct HypothesisResult {
    gift_slug: String,
    gift_title: String,
    hypothesis_id: usize,
    hypothesis_name: &'static str,
    combos_count: usize,
    warmup_time_ms: u128,
    detection_latency_ms: Option<u128>,
    detected_at_request: usize,
    detected_by_viewer: usize,
    detected_combo_idx: usize,
    timed_out: bool,
}

#[inline(always)]
fn create_model_attr(sticker_id: i64) -> StarGiftAttributeId {
    StarGiftAttributeId::Model(StarGiftAttributeIdModel {
        document_id: sticker_id,
    })
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
        limit: 3,
    }
}

// -------------------------------------------------------------
// HYPOTHESIS GENERATORS
// -------------------------------------------------------------

// H1: Baseline Root Feed (No attributes)
fn gen_baseline(gift_id: i64, _all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    vec![create_request(gift_id, None)]
}

// H2: All Models Vector
fn gen_all_models(gift_id: i64, all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    if all_stickers.is_empty() {
        return vec![create_request(gift_id, None)];
    }
    let all_attrs = all_stickers.iter().map(|&id| create_model_attr(id)).collect();
    vec![create_request(gift_id, Some(all_attrs))]
}

// H3: Skip 1 Combinations
fn gen_skip_1(gift_id: i64, all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    let size = all_stickers.len();
    if size <= 1 {
        return vec![create_request(gift_id, None)];
    }
    let mut combos = Vec::with_capacity(size);
    for skip in 0..size {
        let mut attrs = Vec::with_capacity(size - 1);
        for (j, &id) in all_stickers.iter().enumerate() {
            if j != skip {
                attrs.push(create_model_attr(id));
            }
        }
        combos.push(create_request(gift_id, Some(attrs)));
    }
    combos
}

// H4: Production Strategy (All + Skip 1 + Skip 2 Adjacent/Opposite + None)
fn gen_production_strategy(gift_id: i64, all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    let size = all_stickers.len();
    if size == 0 {
        return vec![create_request(gift_id, None)];
    }
    let mut combos = Vec::new();

    // 1. All
    let all_attrs = all_stickers.iter().map(|&id| create_model_attr(id)).collect();
    combos.push(create_request(gift_id, Some(all_attrs)));

    // 2. Skip 1
    for skip in 0..size {
        let mut attrs = Vec::with_capacity(size - 1);
        for (j, &id) in all_stickers.iter().enumerate() {
            if j != skip {
                attrs.push(create_model_attr(id));
            }
        }
        combos.push(create_request(gift_id, Some(attrs)));
    }

    // 3. Skip 2 (Adjacent & Opposite)
    if size >= 4 {
        for skip1 in 0..size {
            let skip2 = (skip1 + 1) % size;
            let mut attrs = Vec::with_capacity(size - 2);
            for (j, &id) in all_stickers.iter().enumerate() {
                if j != skip1 && j != skip2 {
                    attrs.push(create_model_attr(id));
                }
            }
            combos.push(create_request(gift_id, Some(attrs)));
        }

        let half = size / 2;
        for skip1 in 0..half {
            let skip2 = skip1 + half;
            let mut attrs = Vec::with_capacity(size - 2);
            for (j, &id) in all_stickers.iter().enumerate() {
                if j != skip1 && j != skip2 {
                    attrs.push(create_model_attr(id));
                }
            }
            combos.push(create_request(gift_id, Some(attrs)));
        }
    }

    // 4. None
    combos.push(create_request(gift_id, None));
    combos
}

// H5: Individual Single Model Filters
fn gen_single_models(gift_id: i64, all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    if all_stickers.is_empty() {
        return vec![create_request(gift_id, None)];
    }
    all_stickers
        .iter()
        .map(|&id| create_request(gift_id, Some(vec![create_model_attr(id)])))
        .collect()
}

// H6: Interleaved Root None + Skip 1
fn gen_interleaved_none_skip1(gift_id: i64, all_stickers: &[i64], _target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    let skip1 = gen_skip_1(gift_id, all_stickers, _target_sticker);
    let mut combos = Vec::with_capacity(skip1.len() * 2);
    for req in skip1 {
        combos.push(create_request(gift_id, None));
        combos.push(req);
    }
    combos
}

// H7: Direct Target Model Filter (Theoretical Lower Bound)
fn gen_target_direct(gift_id: i64, _all_stickers: &[i64], target_sticker: Option<i64>) -> Vec<GetResaleStarGifts> {
    if let Some(target_id) = target_sticker {
        vec![
            create_request(gift_id, Some(vec![create_model_attr(target_id)])),
            create_request(gift_id, None),
        ]
    } else {
        vec![create_request(gift_id, None)]
    }
}

static HYPOTHESES: &[HypothesisDef] = &[
    HypothesisDef {
        id: 1,
        name: "H1: Root Feed (No Attributes)",
        description: "1 request (attributes: None). Baseline market feed invalidation.",
        generator: gen_baseline,
    },
    HypothesisDef {
        id: 2,
        name: "H2: All Models Vector",
        description: "1 request (all sticker IDs). Checks if full model vector invalidates differently.",
        generator: gen_all_models,
    },
    HypothesisDef {
        id: 3,
        name: "H3: Skip 1 Combinations",
        description: "N requests (each drops 1 sticker). Broad collection coverage without combo explosion.",
        generator: gen_skip_1,
    },
    HypothesisDef {
        id: 4,
        name: "H4: Production Strategy",
        description: "All + Skip 1 + Skip 2 Adjacent/Opposite + None (~2.5N combos). Current production filtering.",
        generator: gen_production_strategy,
    },
    HypothesisDef {
        id: 5,
        name: "H5: Individual Single Model Filters",
        description: "N requests (each queries 1 sticker directly). Tests Telegram per-sticker cache shards.",
        generator: gen_single_models,
    },
    HypothesisDef {
        id: 6,
        name: "H6: Interleaved Root + Skip 1",
        description: "Alternates None and Skip 1 queries. Mixes global feed updates with model filters.",
        generator: gen_interleaved_none_skip1,
    },
    HypothesisDef {
        id: 7,
        name: "H7: Target Model Direct (Baseline Upper Bound)",
        description: "Direct query for target sticker. Determines theoretical maximum speed from Telegram.",
        generator: gen_target_direct,
    },
];

// -------------------------------------------------------------
// MAIN & BENCHMARK LOGIC
// -------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info,ferogram=error")
        .init();

    println!("============================================================================");
    println!("       TELEGRAM STAR GIFT HYPOTHESIS BENCHMARK (MARKET DETECTION LATENCY)   ");
    println!("============================================================================");

    let args: Vec<String> = env::args().collect();
    let test_price: i64 = get_arg_value(&args, "--price").and_then(|s| s.parse().ok()).unwrap_or(50_000);
    let cooldown_secs: u64 = get_arg_value(&args, "--cooldown").and_then(|s| s.parse().ok()).unwrap_or(600); // 10 min default
    let timeout_secs: u64 = get_arg_value(&args, "--timeout").and_then(|s| s.parse().ok()).unwrap_or(180);
    let poll_interval_ms: u64 = get_arg_value(&args, "--poll-ms").and_then(|s| s.parse().ok()).unwrap_or(1610);
    let target_slug_filter = get_arg_value(&args, "--slug");
    let requested_buyer_id: Option<i32> = get_arg_value(&args, "--buyer-id")
        .or_else(|| get_arg_value(&args, "--buyer"))
        .and_then(|s| s.parse().ok());
    let list_only = args.iter().any(|a| a == "--list" || a == "--list-gifts");
    let proxy_arg = get_arg_value(&args, "--proxy").or_else(|| get_arg_value(&args, "--proxy-link")).or_else(|| env::var("TG_PROXY").ok());
    let socks5_arg = get_arg_value(&args, "--socks5").or_else(|| env::var("SOCKS5_PROXY").ok());

    let chosen_hypotheses: Option<Vec<usize>> = get_arg_value(&args, "--hypotheses").map(|s| {
        s.split(',')
            .filter_map(|p| p.trim().parse::<usize>().ok())
            .collect()
    });

    let config = Arc::new(AppConfig::load());
    let pg_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    let active_accounts = get_active_accounts(&pg_pool)
        .await
        .context("Failed to fetch active accounts from DB")?;

    println!("\nActive Accounts in DB ({}):", active_accounts.len());
    for acc in &active_accounts {
        println!("  • ID: {:<3} | Role: {:<7} | Phone: {:?}", acc.id, format!("{:?}", acc.role), acc.phone);
    }

    // Determine buyer account
    let buyer_acc: Account = if let Some(buyer_id) = requested_buyer_id {
        println!("\n[Init] Looking up specified Buyer Account ID {} in database...", buyer_id);
        get_account_by_id(&pg_pool, buyer_id)
            .await?
            .with_context(|| format!("Account with ID {} was not found in DB!", buyer_id))?
    } else {
        let first_buyer = active_accounts.iter().find(|a| matches!(a.role, AccountRole::Buyer))
            .context("No active buyer account found in DB! Specify one via --buyer-id <ID>")?;
        println!("\n[Init] Auto-selected Buyer Account ID {} (Phone: {:?}). (To use a different buyer, specify --buyer-id <ID>)",
            first_buyer.id, first_buyer.phone);
        (*first_buyer).clone()
    };

    let viewer_accs: Vec<&Account> = active_accounts.iter()
        .filter(|a| matches!(a.role, AccountRole::Viewer) && a.id != buyer_acc.id)
        .collect();

    if viewer_accs.is_empty() {
        bail!("No active viewer accounts found in DB! Add active viewers first.");
    }

    if let Some(ref p) = proxy_arg {
        println!("[Proxy] Using MTProxy link: {}", p);
    } else if let Some(ref s) = socks5_arg {
        println!("[Proxy] Using SOCKS5 proxy: {}", s);
    }

    println!("[Init] Connecting Buyer client (ID: {}, Phone: {:?})...", buyer_acc.id, buyer_acc.phone);
    let buyer_client = connect_account_with_proxy(&buyer_acc, &config, proxy_arg.as_deref(), socks5_arg.as_deref())
        .await
        .context("Failed to connect buyer client")?;

    println!("[Init] Connecting {} Viewer client(s)...", viewer_accs.len());
    let mut viewers = Vec::new();
    for (i, v_acc) in viewer_accs.iter().enumerate() {
        match connect_account_with_proxy(v_acc, &config, proxy_arg.as_deref(), socks5_arg.as_deref()).await {
            Ok(cli) => {
                viewers.push(cli);
            }
            Err(e) => {
                println!("  Viewer #{} (ID: {}, Phone: {:?}) failed to connect: {e}", i + 1, v_acc.id, v_acc.phone);
            }
        }
    }
    println!("  ✓ Successfully connected {} viewer(s) ready for benchmarking.", viewers.len());

    if viewers.is_empty() {
        bail!("Failed to connect any viewer clients!");
    }

    println!("[Init] Fetching buyer's gifts from Telegram (Account ID {})...", buyer_acc.id);
    let saved_gifts_res = buyer_client.invoke(&GetSavedStarGifts {
        exclude_unsaved: false,
        exclude_saved: false,
        exclude_unlimited: false, // Return ALL gifts (both unique and regular)
        exclude_unique: false,
        sort_by_value: false,
        exclude_upgradable: false,
        exclude_unupgradable: false,
        peer_color_available: false,
        exclude_hosted: false,
        peer: InputPeer::PeerSelf,
        collection_id: None,
        offset: String::new(),
        limit: 100,
    }).await.context("Failed to get saved star gifts for buyer")?;

    let SavedStarGifts::SavedStarGifts(saved_gifts) = saved_gifts_res;
    let mut owned_unique_gifts = Vec::new();
    let mut regular_gifts_count = 0;

    println!("[Init] Telegram returned {} total gifts on account ID {}:", saved_gifts.gifts.len(), buyer_acc.id);

    for saved_item in saved_gifts.gifts {
        let SavedStarGift::SavedStarGift(item) = saved_item;
        match item.gift {
            StarGift::Unique(unique) => {
                let mut sticker_id = None;
                let mut currently_listed = false;
                let mut listed_price = None;

                if let Some(amounts) = unique.resell_amount.as_ref() {
                    for amount in amounts {
                        if let StarsAmount::StarsAmount(stars) = amount
                            && stars.amount > 0 {
                            currently_listed = true;
                            listed_price = Some(stars.amount);
                        }
                    }
                }

                for attr in &unique.attributes {
                    if let StarGiftAttribute::Model(m) = attr
                        && let Document::Document(doc) = &m.document {
                        sticker_id = Some(doc.id);
                    }
                }

                let status = if currently_listed {
                    format!("LISTED ({} ⭐)", listed_price.unwrap_or(0))
                } else {
                    "Not Listed".to_string()
                };
                println!("  ✓ [Unique NFT Gift] {} (#{}) | Slug: {} | Status: {}",
                    unique.title, unique.num, unique.slug, status);

                owned_unique_gifts.push(TestGiftInfo {
                    slug: unique.slug,
                    title: unique.title,
                    num: unique.num,
                    gift_id: unique.gift_id,
                    sticker_id,
                    currently_listed,
                    listed_price,
                });
            }
            StarGift::StarGift(reg) => {
                regular_gifts_count += 1;
                println!("  ℹ [Regular Gift] ID: {} | Stars: {} | (Note: regular gifts cannot be listed on market until converted/upgraded to NFT)",
                    reg.id, reg.stars);
            }
        }
    }

    if owned_unique_gifts.is_empty() {
        if regular_gifts_count > 0 {
            eprintln!("\n[Attention] Account #{} has {} Regular StarGifts, but 0 Unique (NFT) StarGifts!",
                buyer_acc.id, regular_gifts_count);
            eprintln!("In Telegram, only Unique StarGifts (NFTs with slug and number, e.g. 'EvilEye-1234') can be listed on the Market.");
            eprintln!("If your gifts on account #{} are regular, you need to upgrade them to Unique in Telegram first,", buyer_acc.id);
            eprintln!("or if your unique gifts are on another account, run with: --buyer-id <ACCOUNT_ID>");
        } else {
            eprintln!("\n[Error] Account #{} has NO gifts at all on Telegram!", buyer_acc.id);
            eprintln!("Check if you intended to use a different account, and pass: --buyer-id <ACCOUNT_ID>");
        }
        bail!("Buyer does not own any Unique StarGifts!");
    }

    println!("\nBuyer owned unique gifts ({}) :", owned_unique_gifts.len());
    println!("{:-<80}", "");
    println!("{:<4} {:<24} {:<8} {:<20} {:<15}", "#", "Title", "Num", "Slug", "Market Status");
    println!("{:-<80}", "");
    for (i, g) in owned_unique_gifts.iter().enumerate() {
        let status = if g.currently_listed {
            format!("LISTED ({} stars)", g.listed_price.unwrap_or(0))
        } else {
            "Not Listed".to_string()
        };
        println!("{:<4} {:<24} #{:<7} {:<20} {:<15}", i + 1, g.title, g.num, g.slug, status);
    }
    println!("{:-<80}", "");

    if list_only {
        println!("Listing complete (--list specified). Exiting.");
        return Ok(());
    }

    // Determine target gifts: either specific slug OR ALL unique gifts owned by the buyer!
    let target_gifts: Vec<TestGiftInfo> = if let Some(ref slug) = target_slug_filter {
        let g = owned_unique_gifts.iter().find(|g| &g.slug == slug)
            .with_context(|| format!("Gift with slug '{}' not found in buyer's inventory", slug))?
            .clone();
        vec![g]
    } else {
        println!("\n[Multi-Gift Mode] All {} Unique Gift(s) on Account #{} will participate in benchmarking!",
            owned_unique_gifts.len(), buyer_acc.id);
        owned_unique_gifts.clone()
    };

    // Setup Ctrl+C emergency delisting handler
    let active_listed_slug = Arc::new(std::sync::Mutex::new(None::<String>));
    let buyer_cleanup_cli = buyer_client.clone();
    let cleanup_slug = Arc::clone(&active_listed_slug);

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n[SHUTDOWN] Intercepted Ctrl+C! Checking if target gift is listed...");
        let maybe_slug = { cleanup_slug.lock().unwrap().take() };
        if let Some(slug) = maybe_slug {
            println!("[SHUTDOWN] Delisting gift '{}' before exit...", slug);
            let _ = delist_gift(&buyer_cleanup_cli, &slug).await;
            println!("[SHUTDOWN] Successfully delisted.");
        }
        std::process::exit(0);
    });

    let hypotheses_to_test: Vec<&HypothesisDef> = HYPOTHESES.iter()
        .filter(|h| {
            if let Some(ref list) = chosen_hypotheses {
                list.contains(&h.id)
            } else {
                true
            }
        })
        .collect();

    println!("\nSelected Hypotheses to Benchmark ({}):", hypotheses_to_test.len());
    for h in &hypotheses_to_test {
        println!("  [{}] {}: {}", h.id, h.name, h.description);
    }

    let viewers_arc: Arc<[Client]> = Arc::from(viewers.into_boxed_slice());
    let mut all_results: Vec<HypothesisResult> = Vec::new();

    // Track when each collection was last delisted so we never wait between DIFFERENT collections!
    let mut last_delisted_per_collection: std::collections::HashMap<i64, Instant> = std::collections::HashMap::new();

    // Build schedule: interleaving hypotheses and gifts so different collections alternate naturally!
    let mut schedule = Vec::new();
    for hyp in &hypotheses_to_test {
        for target_gift in &target_gifts {
            schedule.push((*hyp, target_gift.clone()));
        }
    }

    let total_tests_count = schedule.len();

    // Pre-check: if any gift is currently listed, delist it before running!
    for target_gift in &target_gifts {
        if target_gift.currently_listed {
            println!("\n[Warn] Target gift '{}' is currently listed! Delisting it first before running tests...", target_gift.slug);
            let _ = delist_gift(&buyer_client, &target_gift.slug).await;
            println!("[Delisted] Done.");
        }
    }

    for (test_idx, (hyp, target_gift)) in schedule.iter().enumerate() {
        println!("\n============================================================================");
        println!("RUNNING TEST {}/{}: Gift '{}' ({}) on {}",
            test_idx + 1, total_tests_count, target_gift.title, target_gift.slug, hyp.name);
        println!("Description: {}", hyp.description);
        println!("============================================================================");

        // -------------------------------------------------------------
        // COOLDOWN CHECK: NO WAITING BETWEEN DIFFERENT COLLECTIONS!
        // -------------------------------------------------------------
        if let Some(&last_delisted_time) = last_delisted_per_collection.get(&target_gift.gift_id) {
            let elapsed = last_delisted_time.elapsed();
            let cd_duration = Duration::from_secs(cooldown_secs);
            if elapsed < cd_duration {
                let remaining = cd_duration - elapsed;
                let rem_mins = remaining.as_secs() / 60;
                let rem_secs = remaining.as_secs() % 60;
                println!("\n----------------------------------------------------------------------------");
                println!("[COOLDOWN] Collection '{}' was listed {:.1} min ago.", target_gift.title, elapsed.as_secs_f64() / 60.0);
                println!("Waiting remaining {:02}m {:02}s before relisting the SAME collection...", rem_mins, rem_secs);
                println!("(Between DIFFERENT collections, tests run immediately with 0s wait!)");
                println!("----------------------------------------------------------------------------");

                let wait_start = Instant::now();
                while wait_start.elapsed() < remaining {
                    let left = remaining.saturating_sub(wait_start.elapsed());
                    let m = left.as_secs() / 60;
                    let s = left.as_secs() % 60;
                    print!("\r[Cooldown Countdown] Ready in: {:02}m {:02}s ...   ", m, s);
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                    sleep(Duration::from_secs(1)).await;
                }
                println!("\n[Cooldown] Cooldown finished! Ready for next test.\n");
            } else {
                println!("\n[COOLDOWN] Collection '{}' was last listed {:.1} min ago (> 10 min). No wait needed! Proceeding immediately.",
                    target_gift.title, elapsed.as_secs_f64() / 60.0);
            }
        } else {
            println!("\n[COOLDOWN] Collection '{}' has not been listed recently. No wait needed (0s cooldown)! Proceeding immediately.",
                target_gift.title);
        }

        // Fetch collection stickers from DB for this gift (regardless of collection active flag)
        let all_stickers = get_collection_stickers_by_gift_id(&pg_pool, target_gift.gift_id).await?;
        println!("  Collection Models in DB: {} stickers for collection tg_id {}", all_stickers.len(), target_gift.gift_id);

        // Generate combinations for this hypothesis
        let requests = (hyp.generator)(target_gift.gift_id, &all_stickers, target_gift.sticker_id);
        let combos_count = requests.len();
        println!("[Combos] Generated {} requests for this hypothesis.", combos_count);

        let requests_arc: Arc<[GetResaleStarGifts]> = Arc::from(requests.into_boxed_slice());

        // -------------------------------------------------------------
        // STEP 1: WARMUP CYCLE (Ensure 1 full round of cached queries across all viewers)
        // -------------------------------------------------------------
        let warmup_start = Instant::now();
        execute_warmup_round(&viewers_arc, &requests_arc, poll_interval_ms).await;
        let warmup_duration = warmup_start.elapsed();
        println!("[Warmup] Complete in {:.2}s. All combos pre-cached on Telegram servers.", warmup_duration.as_secs_f64());

        // Ensure small sync gap before listing
        sleep(Duration::from_millis(500)).await;

        // -------------------------------------------------------------
        // STEP 2: BUYER LISTS THE GIFT (With auto step-down retry)
        // -------------------------------------------------------------
        let mut actual_list_price = test_price;
        let mut list_succeeded = false;

        while actual_list_price >= 1_000 {
            println!("[Action] Buyer listing gift '{}' for {} stars...", target_gift.slug, actual_list_price);
            let list_res = buyer_client.invoke(&UpdateStarGiftPrice {
                stargift: InputSavedStarGift::Slug(InputSavedStarGiftSlug {
                    slug: target_gift.slug.clone(),
                }),
                resell_amount: StarsAmount::StarsAmount(StarsAmountData {
                    amount: actual_list_price,
                    nanos: 0,
                }),
            }).await;

            match list_res {
                Ok(_) => {
                    list_succeeded = true;
                    break;
                }
                Err(InvocationError::Rpc(ref rpc_err)) if rpc_err.name == "RESELL_STARS_TOO_MUCH" => {
                    let next_price = if actual_list_price > 100_000 {
                        100_000
                    } else if actual_list_price > 50_000 {
                        50_000
                    } else if actual_list_price > 25_000 {
                        25_000
                    } else if actual_list_price > 10_000 {
                        10_000
                    } else if actual_list_price > 5_000 {
                        5_000
                    } else {
                        1_000
                    };
                    println!("[Price Adj] {} stars exceeds Telegram's limit (RESELL_STARS_TOO_MUCH). Stepping down to {} stars...", actual_list_price, next_price);
                    actual_list_price = next_price;
                }
                Err(e) => {
                    eprintln!("[Error] Failed to list gift: {e:#}");
                    break;
                }
            }
        }

        if !list_succeeded {
            eprintln!("Skipping hypothesis due to listing error.");
            continue;
        }

        // Register listed gift for emergency Ctrl+C cleanup
        *active_listed_slug.lock().unwrap() = Some(target_gift.slug.clone());

        let t_listed = Instant::now();
        println!("[Action] Gift '{}' listed at t=0ms! Viewers are now polling to measure detection latency...", target_gift.slug);

        // -------------------------------------------------------------
        // STEP 3: DETECTION POLLING LOOP
        // -------------------------------------------------------------
        let detected = Arc::new(AtomicBool::new(false));
        let total_requests_done = Arc::new(AtomicUsize::new(0));
        let detected_viewer_idx = Arc::new(AtomicUsize::new(0));
        let detected_combo_idx = Arc::new(AtomicUsize::new(0));
        let detection_latency_ms = Arc::new(AtomicUsize::new(0));

        let num_viewers = viewers_arc.len();
        let timeout_duration = Duration::from_secs(timeout_secs);
        let poll_interval = Duration::from_millis(poll_interval_ms);

        let mut handles = Vec::with_capacity(num_viewers);

        for viewer_idx in 0..num_viewers {
            let client = viewers_arc[viewer_idx].clone();
            let reqs = Arc::clone(&requests_arc);
            let is_detected = Arc::clone(&detected);
            let req_counter = Arc::clone(&total_requests_done);
            let det_viewer = Arc::clone(&detected_viewer_idx);
            let det_combo = Arc::clone(&detected_combo_idx);
            let det_latency = Arc::clone(&detection_latency_ms);
            let target_slug = target_gift.slug.clone();

            let stagger = poll_interval.mul_f64(viewer_idx as f64 / num_viewers as f64);

            let handle = tokio::spawn(async move {
                sleep(stagger).await;
                let mut interval = tokio::time::interval(poll_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                let mut local_idx = viewer_idx;
                while !is_detected.load(Ordering::Relaxed) && t_listed.elapsed() < timeout_duration {
                    interval.tick().await;
                    if is_detected.load(Ordering::Relaxed) {
                        break;
                    }

                    let combo_idx = local_idx % reqs.len();
                    local_idx += num_viewers;
                    let req_num = req_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    let res = client.invoke(&reqs[combo_idx]).await;
                    match res {
                        Ok(ResaleStarGifts::ResaleStarGifts(resale)) => {
                            for gift in resale.gifts {
                                if let StarGift::Unique(unique) = gift
                                    && unique.slug == target_slug {
                                    let elapsed = t_listed.elapsed();
                                    det_latency.store(elapsed.as_millis() as usize, Ordering::SeqCst);
                                    det_viewer.store(viewer_idx + 1, Ordering::SeqCst);
                                    det_combo.store(combo_idx, Ordering::SeqCst);
                                    is_detected.store(true, Ordering::SeqCst);
                                    println!(
                                        "\n>>> [TARGET FOUND!] Detected by Viewer #{} on combo #{} after {:.2}s ({} ms, request #{}) <<<",
                                        viewer_idx + 1,
                                        combo_idx,
                                        elapsed.as_secs_f64(),
                                        elapsed.as_millis(),
                                        req_num
                                    );
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(sec) = e.flood_wait_seconds() {
                                eprintln!("[FloodWait] Viewer #{} received flood wait {}s", viewer_idx + 1, sec);
                                sleep(Duration::from_secs(sec + 1)).await;
                            }
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for detection or timeout
        loop {
            if detected.load(Ordering::Relaxed) {
                break;
            }
            if t_listed.elapsed() >= timeout_duration {
                println!("\n[TIMEOUT] Gift was not detected within {}s timeout!", timeout_secs);
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        for h in handles {
            h.abort();
        }

        let is_found = detected.load(Ordering::Relaxed);
        let latency_ms = if is_found {
            Some(detection_latency_ms.load(Ordering::Relaxed) as u128)
        } else {
            None
        };

        // -------------------------------------------------------------
        // STEP 4: DELIST GIFT IMMEDIATELY
        // -------------------------------------------------------------
        println!("[Cleanup] Delisting gift '{}' from market...", target_gift.slug);
        if let Err(e) = delist_gift(&buyer_client, &target_gift.slug).await {
            eprintln!("[Critical Error] Failed to delist gift: {e:#}. Please manually remove it!");
        } else {
            println!("[Cleanup] Gift successfully removed from sale.");
            *active_listed_slug.lock().unwrap() = None;
        }

        // Record that this collection was delisted right now!
        last_delisted_per_collection.insert(target_gift.gift_id, Instant::now());

        // Record result
        all_results.push(HypothesisResult {
            gift_slug: target_gift.slug.clone(),
            gift_title: target_gift.title.clone(),
            hypothesis_id: hyp.id,
            hypothesis_name: hyp.name,
            combos_count,
            warmup_time_ms: warmup_duration.as_millis(),
            detection_latency_ms: latency_ms,
            detected_at_request: total_requests_done.load(Ordering::Relaxed),
            detected_by_viewer: detected_viewer_idx.load(Ordering::Relaxed),
            detected_combo_idx: detected_combo_idx.load(Ordering::Relaxed),
            timed_out: !is_found,
        });
    }

    // -------------------------------------------------------------
    // FINAL RESULTS & COMPARISON
    // -------------------------------------------------------------
    println!("\n\n===============================================================================================================");
    println!("                                      FINAL HYPOTHESIS BENCHMARK RESULTS                                       ");
    println!("===============================================================================================================");
    println!("{:<18} {:<4} {:<36} {:<8} {:<12} {:<15} {:<10}", "Gift", "ID", "Hypothesis Name", "Combos", "Warmup (s)", "Latency (ms)", "Status");
    println!("{:-<110}", "");

    for r in &all_results {
        let warmup_str = format!("{:.2}", r.warmup_time_ms as f64 / 1000.0);
        let latency_str = match r.detection_latency_ms {
            Some(ms) => format!("{} ms ({:.2}s)", ms, ms as f64 / 1000.0),
            None => "TIMEOUT".to_string(),
        };
        let status = if r.timed_out { "TIMEOUT" } else { "DETECTED" };
        println!("{:<18} {:<4} {:<36} {:<8} {:<12} {:<15} {:<10}",
            format!("{} ({})", r.gift_title, r.gift_slug), r.hypothesis_id, r.hypothesis_name, r.combos_count, warmup_str, latency_str, status);
    }
    println!("{:-<110}", "");

    // Identify overall winner (fastest detected latency)
    let winner = all_results.iter()
        .filter(|r| !r.timed_out && r.detection_latency_ms.is_some())
        .min_by_key(|r| r.detection_latency_ms.unwrap());

    if let Some(w) = winner {
        println!("\n🏆 FASTEST HYPOTHESIS OVERALL: {} (on Gift '{}')", w.hypothesis_name, w.gift_slug);
        println!("   Latency:        {} ms ({:.3}s)", w.detection_latency_ms.unwrap(), w.detection_latency_ms.unwrap() as f64 / 1000.0);
        println!("   Combinations:   {}", w.combos_count);
        println!("   Caught on req:  #{} (Combo #{}, Viewer #{})", w.detected_at_request, w.detected_combo_idx, w.detected_by_viewer);
    } else {
        println!("\nNo hypothesis succeeded in detecting the gift within the timeout.");
    }

    println!("===============================================================================================================\n");
    Ok(())
}

async fn execute_warmup_round(viewers: &[Client], requests: &[GetResaleStarGifts], poll_ms: u64) {
    let n_viewers = viewers.len();
    let n_requests = requests.len();
    let total_warmup_items = n_requests.max(n_viewers);

    println!("[Warmup] Warming up all {} viewers across all {} combos ({} total warmup queries)...",
        n_viewers, n_requests, total_warmup_items);

    let counter = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let viewers_active = Arc::new(AtomicUsize::new(0));

    let poll_interval = Duration::from_millis(poll_ms);
    let stagger_step = poll_interval.mul_f64(1.0 / n_viewers as f64);

    let mut handles = Vec::with_capacity(n_viewers);

    for viewer_idx in 0..n_viewers {
        let client = viewers[viewer_idx].clone();
        let counter_ref = Arc::clone(&counter);
        let completed_ref = Arc::clone(&completed);
        let viewers_active_ref = Arc::clone(&viewers_active);
        let reqs = requests.to_vec();

        let handle = tokio::spawn(async move {
            sleep(stagger_step.mul_f64(viewer_idx as f64)).await;
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut did_at_least_one = false;
            loop {
                let idx = counter_ref.fetch_add(1, Ordering::Relaxed);
                if idx >= total_warmup_items && did_at_least_one {
                    break;
                }

                interval.tick().await;
                let combo_idx = idx % reqs.len();
                let _ = client.invoke(&reqs[combo_idx]).await;
                completed_ref.fetch_add(1, Ordering::Relaxed);

                if !did_at_least_one {
                    did_at_least_one = true;
                    viewers_active_ref.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    println!("[Warmup] 100% complete: {} queries executed, all {} viewers engaged and warmed.",
        completed.load(Ordering::Relaxed), viewers_active.load(Ordering::Relaxed));
}

async fn delist_gift(client: &Client, slug: &str) -> Result<()> {
    for attempt in 1..=3 {
        let res = client.invoke(&UpdateStarGiftPrice {
            stargift: InputSavedStarGift::Slug(InputSavedStarGiftSlug {
                slug: slug.to_string(),
            }),
            resell_amount: StarsAmount::StarsAmount(StarsAmountData {
                amount: 0,
                nanos: 0,
            }),
        }).await;

        match res {
            Ok(_) => return Ok(()),
            Err(e) => {
                eprintln!("[Delist] Attempt {} failed: {e}. Retrying in 1s...", attempt);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    bail!("Failed to delist gift after 3 attempts!");
}

async fn connect_account_with_proxy(
    account: &Account,
    config: &AppConfig,
    proxy_link: Option<&str>,
    socks5: Option<&str>,
) -> Result<Client, InvocationError> {
    let mut builder = Client::builder()
        .api_id(account.api_id.unwrap_or(config.default_api_id))
        .api_hash(account.api_hash.clone().unwrap_or(config.default_api_hash.clone()))
        .update_queue_capacity(0)
        .session_string(account.session.clone())
        .update_overflow_strategy(OverflowStrategy::DropNewest)
        .retry_policy(Arc::new(NoRetries))
        .transport(TransportKind::Abridged)
        .system_lang_code("en")
        .device_model(account.device_model.as_deref().unwrap_or("Android"))
        .system_version(account.system_version.as_deref().unwrap_or("16"))
        .app_version(account.app_version.as_deref().unwrap_or("3.4.5"));

    if let Some(link) = proxy_link {
        if !link.is_empty() {
            builder = builder.proxy_link(link);
        }
    } else if let Some(socks) = socks5
        && !socks.is_empty() {
            if let Some((addr, user, pass)) = parse_socks_auth(socks) {
                builder = builder.socks5_auth(addr, user, pass);
            } else {
                builder = builder.socks5(socks);
            }
        }

    let config_client = builder.build().unwrap();
    let (client, _) = Client::connect(config_client).await?;
    Ok(client)
}

fn parse_socks_auth(socks_str: &str) -> Option<(String, String, String)> {
    // Supports user:pass@host:port or host:port:user:pass
    if let Some((auth, addr)) = socks_str.split_once('@')
        && let Some((user, pass)) = auth.split_once(':') {
            return Some((addr.to_string(), user.to_string(), pass.to_string()));
        }
    let parts: Vec<&str> = socks_str.split(':').collect();
    if parts.len() == 4 {
        let addr = format!("{}:{}", parts[0], parts[1]);
        return Some((addr, parts[2].to_string(), parts[3].to_string()));
    }
    None
}

fn get_arg_value(args: &[String], key: &str) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        if arg == key {
            return args.get(i + 1).cloned();
        }
        if let Some(val) = arg.strip_prefix(&format!("{key}=")) {
            return Some(val.to_string());
        }
    }
    None
}
