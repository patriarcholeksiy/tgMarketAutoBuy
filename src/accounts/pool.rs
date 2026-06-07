use crate::accounts::db::{get_active_accounts, Account, AccountRole};
use ferogram::{Client, OverflowStrategy};
use sqlx::PgPool;
use std::env;
use std::sync::Arc;
use std::time::Duration;

pub struct AccountPool {
    buyers: Arc<Vec<Client>>,
    viewers: Arc<Vec<Client>>
}


impl AccountPool {
    pub async fn init(pg_pool: &PgPool) -> Self {
        let accounts = get_active_accounts(pg_pool).await.unwrap();

        let mut buyers = Vec::new();
        let mut viewers = Vec::new();
        for account in accounts {
            let client = Self::connect_client(&account).await;
            match account.role {
                AccountRole::Buyer => buyers.push(client),
                AccountRole::Viewer => viewers.push(client)
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        AccountPool {
            buyers: Arc::new(buyers),
            viewers: Arc::new(viewers)
        }
    }

    async fn connect_client(account: &Account) -> Client {
        let (client, _) = Client::builder()
            .api_id(account.api_id.unwrap_or_else(
                || env::var("DEFAULT_API_ID").unwrap().parse().unwrap()
            ))
            .api_hash(account.api_hash.clone().unwrap_or_else(
                || env::var("DEFAULT_API_HASH").unwrap()
            ))
            .update_queue_capacity(0)
            .session_string(account.session.clone())
            .update_overflow_strategy(OverflowStrategy::DropNewest)
            .connect().await.unwrap();
        client
    }

    #[inline(always)]
    pub async fn get_buyers_client(&self, index: usize) -> &Client {
        &self.buyers[index % self.buyers.len()]
    }

    #[inline(always)]
    pub async fn get_viewer_client(&self, index: usize) -> &Client {
        &self.viewers[index % self.viewers.len()]
    }
}