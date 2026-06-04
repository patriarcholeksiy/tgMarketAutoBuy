use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

fn parse_string_to_f32<'de, D>(deserializer: D) -> Result<f32, D::Error>
where D: Deserializer<'de> {
    let s = String::deserialize(deserializer)?;
    s.parse::<f32>().map_err(serde::de::Error::custom)
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct XGiftItem {
    #[serde(alias = "model", alias = "backdrop")]
    pub name: String,
    #[serde(deserialize_with = "parse_string_to_f32")]
    pub floor_price_ton: f32,
    #[serde(deserialize_with = "parse_string_to_f32")]
    pub avg30d_price: f32,
    #[serde(deserialize_with = "parse_string_to_f32")]
    pub avg60d_price: f32,
    #[serde(deserialize_with = "parse_string_to_f32")]
    pub avg90d_price: f32,
    pub deals30d_count: u16,
    pub deals60d_count: u16,
    pub deals90d_count: u16
}

// #[derive(Deserialize, Debug)]
// #[serde(rename_all = "camelCase")]
// pub struct XGiftBackdrop {
//     pub backdrop: String,
//     #[serde(deserialize_with = "parse_string_to_f32")]
//     pub floor_price_ton: f32,
//     #[serde(deserialize_with = "parse_string_to_f32")]
//     pub avg30d_price: f32,
//     #[serde(deserialize_with = "parse_string_to_f32")]
//     pub avg60d_price: f32,
//     #[serde(deserialize_with = "parse_string_to_f32")]
//     pub avg90d_price: f32,
//     pub deals30d_count: u16,
//     pub deals60d_count: u16,
//     pub deals90d_count: u16
// }

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct XGiftResponse {
    pub gift_model: Vec<XGiftItem>,
    pub gift_backdrop: Vec<XGiftItem>
}

pub struct XGiftResponseMap {
    pub models: HashMap<String, XGiftItem>,
    pub backdrops: HashMap<String, XGiftItem>
}

impl XGiftResponse {
    pub fn into_map(self) -> XGiftResponseMap {
        XGiftResponseMap {
            models: self.gift_model
                .into_iter()
                .map(|m| (m.name.clone(), m))
                .collect(),
            backdrops: self.gift_backdrop
                .into_iter()
                .map(|b| (b.name.clone(), b))
                .collect(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PortalsItem {
    pub name: String,
    pub floor_price: Option<String>
}

// #[derive(Deserialize, Debug)]
// pub struct BackdropPortals {
//     pub name: String,
//     pub floor_price: Option<String>
// }

#[derive(Deserialize, Debug)]
pub struct CollectionPortals {
    pub models: Vec<PortalsItem>,
    pub backdrops: Vec<PortalsItem>
}

#[derive(Deserialize, Debug)]
pub struct PortalsResponse {
    pub collections: HashMap<String, CollectionPortals>
}

#[derive(Deserialize, Debug)]
pub struct CollectionPortalsMap {
    pub models: HashMap<String, f32>,
    pub backdrops: HashMap<String, f32>
}

#[derive(Deserialize, Debug)]
pub struct PortalsResponseMap {
    pub collections: HashMap<String, CollectionPortalsMap>
}

impl PortalsResponse {
    pub fn into_map(self) -> PortalsResponseMap {
        let mut map_collections = HashMap::with_capacity(self.collections.len());

        for (col_name, raw_col) in self.collections {
            let map_col = CollectionPortalsMap {
                models: raw_col.models
                    .into_iter()
                    .filter_map(|m| {
                        if let Some(floor) = m.floor_price {
                            match floor.parse::<f32>() {
                                Ok(floor) => Some((m.name, floor)),
                                Err(_) => {
                                    println!("Bad floor for {}, {}, {}", m.name, col_name, floor);
                                    None
                                }
                            }
                        } else {
                            println!("model doesn't have '{}' (collection: {}) no price (None)", m.name, col_name);
                            None
                        }
                    })
                    .collect(),
                backdrops: raw_col.backdrops
                    .into_iter()
                    .filter_map(|b| {
                        if let Some(floor) = b.floor_price {
                            match floor.parse::<f32>() {
                                Ok(floor) => Some((b.name, floor)),
                                Err(_) => {
                                    println!("Bad floor for {}, {}, {}", b.name, col_name, floor);
                                    None
                                }
                            }
                        } else {
                            println!("backdrop doesn't have '{}' (collection: {}) no price (None)", b.name, col_name);
                            None
                        }
                    })
                    .collect(),
            };
            map_collections.insert(col_name, map_col);
        }

        PortalsResponseMap { collections: map_collections }
    }
}

#[derive(Deserialize, Debug)]
pub struct BybitResponse {
    pub result: BybitResult
}

#[derive(Deserialize, Debug)]
pub struct BybitResult {
    pub list: Vec<BybitTicker>
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BybitTicker {
    pub last_price: String
}