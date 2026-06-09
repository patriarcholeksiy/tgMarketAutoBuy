use ferogram::tl::enums::{Document, StarGift, StarGiftAttribute, StarsAmount};

pub struct ParsedGift {
    pub sticker_id: i64,
    pub backdrop_id: i32,
    pub price_in_stars: i64,
    pub slug: String
}

impl ParsedGift {
    // TODO: принимать максимальный баланс и если его достаточно для покупки подарка, парсить model и backdrop (если не навредит скорости в branch predict)
    pub fn try_from_tg(gift: StarGift) -> Option<Self> {
        if let StarGift::Unique(unique) = gift {
            let mut sticker_id = None;
            let mut backdrop_id = None;
            let mut price_in_stars = None;

            if let Some(amounts) = unique.resell_amount.as_ref() {
                for amount in amounts {
                    if let StarsAmount::StarsAmount(a) = amount {
                        price_in_stars = Some(a.amount);
                        break
                    }
                }
            }
            
            if let [a1, _, a3] = unique.attributes.as_slice() {
                if let StarGiftAttribute::Model(model) = a1 {
                    if let Document::Document(doc) = &model.document {
                        sticker_id = Some(doc.id);
                    }
                }
                if let StarGiftAttribute::Backdrop(backdrop) = a3 {
                    backdrop_id = Some(backdrop.backdrop_id);
                }
            } else {
                tracing::warn!("Linear patter [a1, _, a3] for fast parse doesn't work with - {}", unique.slug);
                for attribute in &unique.attributes {
                    match attribute {
                        StarGiftAttribute::Model(model) => {
                            if let Document::Document(doc) = &model.document {
                                sticker_id = Some(doc.id);
                            }
                        }
                        StarGiftAttribute::Backdrop(backdrop) => {
                            backdrop_id = Some(backdrop.backdrop_id)
                        },
                        _ => {}
                    }
                }
            }

            if let (Some(s_id), Some(b_id), Some(price))
                = (sticker_id, backdrop_id, price_in_stars) {
                return Some(ParsedGift {
                    sticker_id: s_id,
                    backdrop_id: b_id,
                    price_in_stars: price,
                    slug: unique.slug,
                })
            }
        };
        None
    }
}