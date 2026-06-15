use std::sync::Arc;
use arc_swap::ArcSwapOption;
use dotenvy::dotenv;
use serde::Deserialize;

fn default_portals_username() -> String { "GiftsToPortals".to_string() }
fn default_portals_id() -> i64 { 7804544881 }

fn default_market_poll_ms() -> usize { 1610 }
fn default_num() -> i64 { 5 }
fn default_den() -> i64 { 2 }

fn default_backdrop_x_gift_reduction_factor() -> f32 { 0.75 }
fn default_min_reliable_deal() -> u16 { 30 }
fn default_floor_price() -> i32 { 125 }
fn default_floor_price_calc() -> f32 { 125.0 }
fn default_star_price_per_dollar() -> f32 { 0.015 }
fn default_historical_price_weight() -> f32 { 0.7 }
fn default_new_api_price_weight() -> f32 { 0.3 }

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database_url: String,

    pub bot_token: String,
    pub admin_user_id: i64,

    #[serde(default = "default_portals_username")]
    pub portals_username: String,
    #[serde(default = "default_portals_id")]
    pub portals_id: i64,

    // default var for auth tg client
    pub default_api_id: i32,
    pub default_api_hash: String,

    pub default_session_string: String,

    #[serde(default = "default_market_poll_ms")]
    pub market_poll_ms: usize,
    // it is necessary to calculate how much the floor
    // of the price should be greater than or equal to
    // the selling price of the gift.
    #[serde(default = "default_num")]
    pub coeff_num: i64,
    #[serde(default = "default_den")]
    pub coeff_den: i64,

    #[serde(default = "default_backdrop_x_gift_reduction_factor")]
    pub backdrop_x_gift_reduction_factor: f32,
    #[serde(default = "default_min_reliable_deal")]
    pub min_reliable_deals: u16,
    #[serde(default = "default_floor_price")]
    pub floor_price_default: i32,
    #[serde(default = "default_floor_price_calc")]
    pub floor_price_for_calc: f32,
    #[serde(default = "default_star_price_per_dollar")]
    pub star_price_per_dollar: f32,
    #[serde(default = "default_historical_price_weight")]
    pub historical_price_weight: f32,
    #[serde(default = "default_new_api_price_weight")]
    pub new_api_price_weight: f32,

    #[serde(skip)]
    pub current_wreq_proxy: Arc<ArcSwapOption<String>>,
}

impl AppConfig {
    // 150_000_000 its 0.15 ton it is necessary if the price is sensitive
    pub const MAX_SLIPPAGE_NANO_TON: i64 = 150_000_000;

    pub fn load() -> Self {
        dotenv().ok();
        envy::from_env::<AppConfig>().expect("Failed to load config")
    }
}