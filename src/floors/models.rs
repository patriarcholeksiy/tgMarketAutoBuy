use sqlx::FromRow;

#[derive(FromRow, Debug)]
pub struct Collection {
    pub id: i32,
    pub title: String,
    pub short_title: String,
    pub tg_id: i64,
    pub active: bool
}

#[derive(FromRow, Debug)]
pub struct Model {
    pub name: String,
    pub collection_id: i32,
    pub floor_price: i32,
    pub sticker_id: i64,
    pub crafted: bool
}

#[derive(FromRow, Debug)]
pub struct Backdrop {
    pub name: String,
    pub collection_id: i32,
    pub floor_price: i32,
    pub tg_backdrop_id: i32
}