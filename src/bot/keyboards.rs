use ferogram::{Button, InlineKeyboard, ReplyKeyboard};

pub fn start_reply_keyboard() -> ReplyKeyboard {
    ReplyKeyboard::new()
        .row([
            Button::text("Gifts and Floors"),
            Button::text("Accounts"),
        ])
        .resize()
}

pub fn service_floors_keyboard() -> InlineKeyboard {
    InlineKeyboard::new()
        .row([
            Button::callback("Update floor cache", b"floor:cache_update"),
            Button::callback("Update floor prices", b"floor:price_update"),
        ])
        .row([
            Button::callback("On proxy for markets", b"floor:on_proxy"),
            Button::callback("Off proxy for markets", b"floor:off_proxy")
        ])
}

pub fn service_accounts_keyboard() -> InlineKeyboard {
    InlineKeyboard::new()
        .row([
            Button::callback("Add new viewers", b"accounts:add_new_viewers"),
        ])
        .row([
            Button::callback("Ping update status", b"accounts:ping_update_status")
        ])
}