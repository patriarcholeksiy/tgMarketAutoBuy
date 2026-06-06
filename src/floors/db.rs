use std::collections::{HashMap, HashSet};
use sqlx::{PgPool, QueryBuilder};
use crate::floors::models::{Backdrop, Collection, Model};


#[derive(Debug)]
pub struct ModelItem {
    pub id: i32,
    pub collection_short_title: String,
    pub name: String,
    pub floor_price: i32
}

pub struct PriceUpdateModel {
    pub id: i32,
    pub new_floor: i32
}

#[derive(Debug)]
pub struct BackdropItem {
    pub id: i32,
    pub collection_short_title: String,
    pub name: String,
    pub floor_price: i32
}

pub struct PriceUpdateBackdrop {
    pub id: i32,
    pub new_floor: i32
}

pub async fn fetch_all_collections(pool: &PgPool) -> Result<Vec<Collection>, sqlx::Error> {
    let collections = sqlx::query_as!(
        Collection,
        "SELECT id, title, short_title, tg_id, active FROM collections"
    ).fetch_all(pool).await?;
    Ok(collections)
}

pub async fn fetch_collections_without_models(pool: &PgPool)
    -> Result<Vec<Collection>, sqlx::Error> {
    let collections = sqlx::query_as!(
        Collection,
        r#"SELECT id, title, short_title, tg_id, active FROM collections WHERE NOT EXISTS (
            SELECT 1 FROM models
            WHERE models.collection_id = collections.id
         )"#
    ).fetch_all(pool).await?;
    Ok(collections)
}

pub async fn fetch_all_tg_ids(pool: &PgPool) -> Result<HashSet<i64>, sqlx::Error> {
    let collection_tg_ids = sqlx::query_scalar!(
        "SELECT tg_id FROM collections"
    ).fetch_all(pool).await?;
    Ok(collection_tg_ids.into_iter().collect::<HashSet<_>>())
}

pub async fn fetch_all_short_titles(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    let short_titles = sqlx::query_scalar!(
        "SELECT short_title FROM collections"
    ).fetch_all(pool).await?;
    Ok(short_titles)
}

pub async fn save_new_collections(pool: &PgPool, new_collections: Vec<Collection>)
    -> Result<(), sqlx::Error> {
    if new_collections.is_empty() {
        return Ok(())
    }
    let mut query_builder = QueryBuilder::new(
        "INSERT INTO collections (title, short_title, tg_id, active)"
    );

    query_builder.push_values(new_collections, |mut b, col| {
        b.push_bind(col.title)
            .push_bind(col.short_title)
            .push_bind(col.tg_id)
            .push_bind(col.active);
    });

    let query = query_builder.build();
    query.execute(pool).await?;

    Ok(())
}

pub async fn save_new_models(pool: &PgPool, models: Vec<Model>) -> Result<(), sqlx::Error> {
    if models.is_empty() { return Ok(()) }

    let mut query_builder = QueryBuilder::new(
        "INSERT INTO models (name, collection_id, floor_price, sticker_id, crafted)"
    );

    query_builder.push_values(models, |mut separated, model| {
        separated
            .push_bind(model.name)
            .push_bind(model.collection_id)
            .push_bind(model.floor_price)
            .push_bind(model.sticker_id)
            .push_bind(model.crafted);
    });

    let query = query_builder.build();
    query.execute(pool).await?;

    Ok(())
}

pub async fn save_new_backdrops(pool: &PgPool, backdrops: Vec<Backdrop>) -> Result<(), sqlx::Error> {
    if backdrops.is_empty() { return Ok(()) }

    let mut query_builder = QueryBuilder::new(
        "INSERT INTO backdrops (name, collection_id, floor_price, tg_backdrop_id)"
    );

    query_builder.push_values(backdrops, |mut separated, backdrop| {
        separated
            .push_bind(backdrop.name)
            .push_bind(backdrop.collection_id)
            .push_bind(backdrop.floor_price)
            .push_bind(backdrop.tg_backdrop_id);
    });

    let query = query_builder.build();
    query.execute(pool).await?;

    Ok(())
}


pub async fn get_all_models_item(pool: &PgPool)
    -> Result<HashMap<(String, String), ModelItem>, sqlx::Error> {
    let records = sqlx::query!(
        r#"
        SELECT
            m.id,
            c.short_title as collection_short_title,
            m.name,
            m.floor_price
        FROM models m
        JOIN collections c ON m.collection_id = c.id
        "#
    ).fetch_all(pool).await?;

    let map = records.into_iter()
        .map(|r| {
            let key = (r.collection_short_title.clone(), r.name.clone());
            let item = ModelItem {
                id: r.id,
                collection_short_title: r.collection_short_title,
                name: r.name,
                floor_price: r.floor_price
            };
            (key, item)
        })
        .collect();

    Ok(map)
}

pub async fn get_all_backdrops_item(pool: &PgPool)
    -> Result<HashMap<(String, String), BackdropItem>, sqlx::Error> {
    let records = sqlx::query!(
        r#"
        SELECT
            b.id,
            c.short_title as collection_short_title,
            b.name,
            b.floor_price
        FROM backdrops b
        JOIN collections c ON b.collection_id = c.id
        "#
    ).fetch_all(pool).await?;

    let map = records.into_iter()
        .map(|r| {
            let key = (r.collection_short_title.clone(), r.name.clone());
            let item = BackdropItem {
                id: r.id,
                collection_short_title: r.collection_short_title,
                name: r.name,
                floor_price: r.floor_price
            };
            (key, item)
        })
        .collect();

    Ok(map)
}

pub async fn bulk_update_models(pool: &PgPool, updates: Vec<PriceUpdateModel>)
    -> Result<(), sqlx::Error> {
    if updates.is_empty() {
        tracing::error!("Updates for models is empty!");
        return Ok(())
    }

    let ids: Vec<i32> = updates.iter().map(|u| u.id).collect();
    let new_floors: Vec<i32> = updates.iter().map(|u| u.new_floor).collect();

    sqlx::query!(
        r#"
        UPDATE models
        SET floor_price = data_table.new_floor
        FROM (
            SELECT
                UNNEST($1::int[]) as update_id,
                UNNEST($2::int[]) as new_floor
        ) AS data_table
        WHERE models.id = data_table.update_id
        "#,
        &ids,
        &new_floors
    ).execute(pool).await?;

    Ok(())
}

pub async fn bulk_update_backdrops(pool: &PgPool, updates: Vec<PriceUpdateBackdrop>)
                                -> Result<(), sqlx::Error> {
    if updates.is_empty() {
        tracing::error!("Updates for backdrops is empty!");
        return Ok(())
    }

    let ids: Vec<i32> = updates.iter().map(|u| u.id).collect();
    let new_floors: Vec<i32> = updates.iter().map(|u| u.new_floor).collect();

    sqlx::query!(
        r#"
        UPDATE backdrops
        SET floor_price = data_table.new_floor
        FROM (
            SELECT
                UNNEST($1::int[]) as update_id,
                UNNEST($2::int[]) as new_floor
        ) AS data_table
        WHERE backdrops.id = data_table.update_id
        "#,
        &ids,
        &new_floors
    ).execute(pool).await?;

    Ok(())
}

