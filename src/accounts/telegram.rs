use ferogram::tl::enums::{Document, StarGift, StarGiftAttribute, StarsAmount};

pub struct ParsedGift {
    pub sticker_id: i64,
    pub backdrop_id: i32,
    pub price_in_stars: Option<i64>,
    pub price_in_ton: Option<i64>,
    pub resale_ton_only: bool,
    pub slug: String,
}

impl ParsedGift {
    #[inline(always)]
    pub fn is_stars(&self) -> bool {
        !self.resale_ton_only && self.price_in_stars.is_some()
    }

    // TODO: ????????? ???????????? ?????? ? ???? ??? ?????????? ??? ??????? ???????, ??????? model ? backdrop (???? ?? ???????? ???????? ? branch predict)
    pub fn try_from_tg(gift: StarGift) -> Option<Self> {
        if let StarGift::Unique(unique) = gift {
            let mut sticker_id = None;
            let mut backdrop_id = None;
            let mut price_in_stars = None;
            let mut price_in_ton = None;

            if let Some(amounts) = unique.resell_amount.as_ref() {
                for amount in amounts {
                    match amount {
                        StarsAmount::StarsAmount(stars) => {
                            price_in_stars = Some(stars.amount);
                        }
                        StarsAmount::StarsTonAmount(ton) => {
                            price_in_ton = Some(ton.amount);
                        }
                    }
                }
            }

            match unique.attributes.as_slice() {
                [a1, _, a3] => {
                    if let StarGiftAttribute::Model(model) = a1 {
                        if let Document::Document(doc) = &model.document {
                            sticker_id = Some(doc.id);
                        }
                    } else { tracing::warn!("Cannot get model for attr count 3") }
                    if let StarGiftAttribute::Backdrop(backdrop) = a3 {
                        backdrop_id = Some(backdrop.backdrop_id);
                    } else { tracing::warn!("Cannot get backdrop for attr count 3") }
                }
                [a1, _, a3, _] => {
                    if let StarGiftAttribute::Model(model) = a1 {
                        if let Document::Document(doc) = &model.document {
                            sticker_id = Some(doc.id);
                        }
                    } else { tracing::warn!("Cannot get model for attr count 4") }
                    if let StarGiftAttribute::Backdrop(backdrop) = a3 {
                        backdrop_id = Some(backdrop.backdrop_id);
                    } else { tracing::warn!("Cannot get backdrop for attr count 4") }
                }
                _ => {
                    tracing::warn!("Abnormal number of attributes for the lot: {}", unique.slug);
                    for attribute in &unique.attributes {
                        match attribute {
                            StarGiftAttribute::Model(model) => {
                                if let Document::Document(doc) = &model.document {
                                    sticker_id = Some(doc.id);
                                }
                            }
                            StarGiftAttribute::Backdrop(backdrop) => {
                                backdrop_id = Some(backdrop.backdrop_id);
                            }
                            _ => {}
                        }
                    }
                }
            }

            if let (Some(s_id), Some(b_id)) = (sticker_id, backdrop_id)
                && (price_in_stars.is_some() || price_in_ton.is_some()) {
                return Some(ParsedGift {
                    sticker_id: s_id,
                    backdrop_id: b_id,
                    price_in_stars,
                    price_in_ton,
                    resale_ton_only: unique.resale_ton_only,
                    slug: unique.slug,
                });
            }
        };
        None
    }
}
