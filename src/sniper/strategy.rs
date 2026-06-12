use crate::config::AppConfig;

pub struct SniperStrategy;

impl SniperStrategy {
    #[inline(always)]
    pub fn should_buy(floor_price: i32,
                      current_price: i64,
                      coeff_num: i64,
                      coeff_den: i64) -> bool {
        let floor = floor_price as i64;
        // floor >= (current_price * config.coeff_num) / config.coeff_den
        (floor * coeff_den) >= (current_price * coeff_num)
    }
}