use crate::accounts::db::{get_active_accounts, Account, AccountRole};
use crate::config::AppConfig;
use anyhow::{bail, Context, Result};
use ferogram::tl::enums::contacts::ResolvedPeer;
use ferogram::tl::enums::{InputPeer, User};
use ferogram::tl::functions::contacts::ResolveUsername;
use ferogram::tl::types::InputPeerUser;
use ferogram::{Client, OverflowStrategy};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

pub struct AccountPool {
    pub buyers: Arc<[(Client, InputPeer)]>,
    pub viewers: Vec<Client>
}

impl AccountPool {
    pub async fn init(pg_pool: &PgPool, config: &AppConfig) -> Result<Self> {
        let accounts = get_active_accounts(pg_pool)
            .await
            .context("Failed to get active accounts from DB")?;

        let mut buyers = Vec::new();
        let mut viewers = Vec::new();
        for account in accounts {
            let client = match Self::connect_client(&account, config).await {
                Ok(client) => client,
                Err(err) => {
                    if matches!(account.role, AccountRole::Buyer) {
                        return Err(err).context(format!("Buyer account {:?} could not log in", account.phone));
                    }
                    tracing::warn!("Viewer account {:?} could not log in: {err:#}", account.phone);
                    continue;
                }
            };
            match account.role {
                AccountRole::Buyer => {
                    let input_peer = Self::generate_input_peer(&client, config)
                        .await
                        .context("Failed generate input peer")?;
                    tracing::info!("Buyer client {:?} logged in", account.phone);
                    buyers.push((client, input_peer))
                },
                AccountRole::Viewer => {
                    tracing::info!("Viewer client {:?} logged in", account.phone);
                    viewers.push(client) }
            }
            tokio::time::sleep(Duration::from_millis(65)).await;
        }

        if buyers.is_empty() {
            bail!("Critical error: No valid buyer accounts were initialized!");
        }
        if viewers.is_empty() {
            bail!("Critical error: No viewers initialized!");
        }

        tracing::info!("Buyers count - {}\nViewers count - {}", buyers.len(), viewers.len());
        Ok(AccountPool {
            buyers: Arc::from(buyers),
            viewers
        })
    }

    async fn connect_client(account: &Account, config: &AppConfig) -> Result<Client> {
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
            .connect().await?;
        Ok(client)
    }

    async fn generate_input_peer(client: &Client, config: &AppConfig) -> Result<InputPeer> {
        let ResolvedPeer::ResolvedPeer(peer) = client.invoke(&ResolveUsername {
            username: config.portals_username.clone(),
            referer: None,
        }).await.context("Failed to invoke Portals resolved peer")?;
        let User::User(user) = peer.users.first().context("Can't get user from peer")? else {
            bail!("Peer user have another enum type")
        };
        if user.id != config.portals_id { 
            bail!("Peer user id is not equal to the declared id")
        }
        Ok(InputPeer::User(InputPeerUser {
            user_id: user.id,
            access_hash: user.access_hash.context("Can't get access_hash from Portals peer")?,
        }))
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