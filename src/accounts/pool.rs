use crate::accounts::db::{get_active_accounts, Account, AccountRole};
use ferogram::{tl, Client, OverflowStrategy};
use sqlx::PgPool;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use ferogram::tl::enums::contacts::ResolvedPeer;
use ferogram::tl::enums::{InputPeer, User};
use ferogram::tl::types::InputPeerUser;
use crate::config::AppConfig;

pub struct AccountPool {
    pub buyers: Arc<[(Client, InputPeer)]>,
    pub viewers: Vec<Client>
}


impl AccountPool {
    pub async fn init(pg_pool: &PgPool, config: &AppConfig) -> Self {
        let accounts = get_active_accounts(pg_pool).await.unwrap();

        let mut buyers = Vec::new();
        let mut viewers = Vec::new();
        for account in accounts {
            let client = Self::connect_client(&account, config).await;
            match account.role {
                AccountRole::Buyer => {
                    let input_peer = Self::generate_input_peer(&client).await;
                    buyers.push((client, input_peer))
                },
                AccountRole::Viewer => viewers.push(client)
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        AccountPool {
            buyers: Arc::from(buyers),
            viewers
        }
    }

    async fn connect_client(account: &Account, config: &AppConfig) -> Client {
        let (client, _) = Client::builder()
            .api_id(account.api_id.unwrap_or_else(
                || config.default_api_id
            ))
            .api_hash(account.api_hash.clone().unwrap_or_else(
                || config.default_api_hash.clone()
            ))
            .update_queue_capacity(0)
            .session_string(account.session.clone())
            .update_overflow_strategy(OverflowStrategy::DropNewest)
            .connect().await.unwrap();
        client
    }

    async fn generate_input_peer(client: &Client) -> InputPeer {
        let peer = client.invoke(&tl::functions::contacts::ResolveUsername {
            username: "GiftsToPortals".to_string(),
            referer: None,
        }).await.unwrap();
        let ResolvedPeer::ResolvedPeer(peer) = peer;
        let User::User(user) = peer.users.get(0).unwrap() else { todo!() };
        InputPeer::User(InputPeerUser {
            user_id: 7804544881,
            access_hash: user.access_hash.expect("access_hash is empty"),
        })
    }

    #[inline(always)]
    pub fn get_buyers_client(&self, index: usize) -> &(Client, InputPeer) {
        &self.buyers[index % self.buyers.len()]
    }

    #[inline(always)]
    pub fn get_viewer_client(&self, index: usize) -> &Client {
        &self.viewers[index % self.viewers.len()]
    }
}