use sqlx::PgPool;

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "account_role", rename_all = "lowercase")]
pub enum AccountRole {
    Viewer,
    Buyer
}

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "account_status", rename_all = "lowercase")]
pub enum AccountStatus {
    Active,
    Frozen,
    Unauthorized,
    DuplicatedKey,
}

#[derive(Debug, Clone)]
pub struct Account {
    pub id: i32,
    pub api_id: Option<i32>,
    pub api_hash: Option<String>,
    pub phone: Option<String>,
    pub session: String,
    pub role: AccountRole,
    pub device_model: Option<String>,
    pub system_version: Option<String>,
    pub app_version: Option<String>,
}

pub async fn get_active_accounts(pg_pool: &PgPool) -> Result<Vec<Account>, sqlx::Error> {
    let accounts = sqlx::query_as!(
        Account,
        r#"
        SELECT
            acc.id,
            app.api_id AS "api_id?",
            app.api_hash AS "api_hash?",
            acc.phone AS "phone?",
            acc.session,
            acc.role AS "role: AccountRole",
            acc.device_model AS "device_model?",
            acc.system_version AS "system_version?",
            acc.app_version AS "app_version?"
        FROM accounts AS acc
        LEFT JOIN telegram_apps AS app ON acc.app_id = app.id
        WHERE acc.status = 'active'
        ORDER BY acc.created_at DESC
        "#
    ).fetch_all(pg_pool).await?;
    Ok(accounts)
}

pub async fn get_new_active_viewers(pg_pool: &PgPool, existing_ids: &[i32])
    -> Result<Vec<Account>, sqlx::Error> {
    let accounts = sqlx::query_as!(
        Account,
        r#"
        SELECT
            acc.id,
            app.api_id AS "api_id?",
            app.api_hash AS "api_hash?",
            acc.phone AS "phone?",
            acc.session,
            acc.role AS "role: AccountRole",
            acc.device_model AS "device_model?",
            acc.system_version AS "system_version?",
            acc.app_version AS "app_version?"
        FROM accounts AS acc
        LEFT JOIN telegram_apps AS app ON acc.app_id = app.id
        WHERE acc.status = 'active'
            AND acc.role = 'viewer'
            AND NOT (acc.id = ANY($1))
        ORDER BY acc.created_at DESC
        "#,
        existing_ids
    ).fetch_all(pg_pool).await?;
    Ok(accounts)
}

pub async fn deactivate_account_by_id(pg_pool: &PgPool, id: i32, status: AccountStatus)
    -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE accounts SET status = $1::account_status WHERE id = $2",
        status as _, id
    ).execute(pg_pool).await?;
    Ok(())
}

pub async fn count_active_viewers(pg_pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM accounts WHERE status = 'active' AND role = 'viewer'"
    ).fetch_one(pg_pool).await
}

pub struct CollectionStickers {
    pub gift_id: i64,
    pub sticker_ids: Vec<i64>
}

pub async fn get_active_collection_stickers(pg_pool: &PgPool)
    -> Result<Vec<CollectionStickers>, sqlx::Error> {
    let collection_stickers = sqlx::query_as!(
        CollectionStickers,
        r#"
        SELECT c.tg_id AS gift_id, ARRAY_AGG(sticker_id) as "sticker_ids!"
        FROM models as m
        JOIN collections AS c ON m.collection_id = c.id
        WHERE c.active = true
        GROUP BY c.tg_id
        "#
    ).fetch_all(pg_pool).await?;

    Ok(collection_stickers)
}
pub async fn get_account_by_id(pg_pool: &PgPool, id: i32) -> Result<Option<Account>, sqlx::Error> {
    let account = sqlx::query_as!(
        Account,
        r#"
        SELECT
            acc.id,
            app.api_id AS "api_id?",
            app.api_hash AS "api_hash?",
            acc.phone AS "phone?",
            acc.session,
            acc.role AS "role: AccountRole",
            acc.device_model AS "device_model?",
            acc.system_version AS "system_version?",
            acc.app_version AS "app_version?"
        FROM accounts AS acc
        LEFT JOIN telegram_apps AS app ON acc.app_id = app.id
        WHERE acc.id = $1
        "#,
        id
    ).fetch_optional(pg_pool).await?;

    Ok(account)
}
