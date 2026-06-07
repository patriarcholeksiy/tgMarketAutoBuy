use sqlx::PgPool;

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "account_role", rename_all = "lowercase")]
pub enum AccountRole {
    Viewer,
    Buyer
}

#[derive(Debug)]
pub struct Account {
    pub api_id: Option<i32>,
    pub api_hash: Option<String>,
    pub phone: Option<String>,
    pub session: String,
    pub role: AccountRole
}

pub async fn get_active_accounts(pg_pool: &PgPool) -> Result<Vec<Account>, sqlx::Error> {
    let accounts = sqlx::query_as!(
        Account,
        r#"
        SELECT
            app.api_id AS "api_id?",
            app.api_hash AS "api_hash?",
            acc.phone,
            acc.session,
            acc.role AS "role: AccountRole"
        FROM accounts AS acc
        LEFT JOIN telegram_apps AS app ON acc.app_id = app.id
        WHERE acc.status = 'active'
        ORDER BY acc.created_at DESC
        "#
    ).fetch_all(pg_pool).await?;
    Ok(accounts)
}