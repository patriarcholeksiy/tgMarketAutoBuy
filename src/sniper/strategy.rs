pub struct SniperStrategy;

impl SniperStrategy {
    #[inline(always)]
    pub fn should_buy(floor_price: i32, current_price: i64) -> bool {
        let floor = floor_price as i64;
        floor >= current_price * 2
    }
}