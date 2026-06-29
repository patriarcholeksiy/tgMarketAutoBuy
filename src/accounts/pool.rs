use crate::accounts::db::{deactivate_account_by_id, get_active_accounts, get_new_active_viewers, Account, AccountRole, AccountStatus};
use crate::config::AppConfig;
use anyhow::{bail, Context, Result};
use arc_swap::ArcSwap;
use ferogram::tl::enums::contacts::ResolvedPeer;
use ferogram::tl::enums::{InputPeer, User};
use ferogram::tl::functions::account::UpdateStatus;
use ferogram::tl::functions::contacts::ResolveUsername;
use ferogram::tl::types::InputPeerUser;
use ferogram::{Client, InvocationError, NoRetries, OverflowStrategy, TransportKind};
use sqlx::PgPool;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Default)]
pub struct ViewersReport {
    pub added: Vec<(i32, Option<String>)>, // id, phone
    pub failed: Vec<(i32, Option<String>, String)>, // id, phone, err
}

#[derive(Clone)]
pub struct ViewerData {
    pub client: Client,
    pub id: i32,
    pub phone: Option<String>,
}

pub struct AccountPool {
    pub buyers: Arc<[(Client, InputPeer)]>,
    pub viewers: ArcSwap<Vec<ViewerData>>,
    pub new_viewers_tx: UnboundedSender<ViewerData>,
    pub notify_tx: UnboundedSender<String>,
}

impl AccountPool {
    pub async fn init(pg_pool: &PgPool, config: &AppConfig, notify_tx: UnboundedSender<String>)
        -> Result<(Self, UnboundedReceiver<ViewerData>)> {
        let accounts = get_active_accounts(pg_pool)
            .await
            .context("Failed to get active accounts from DB")?;

        let mut buyers = Vec::new();
        let mut viewers = Vec::new();
        for account in accounts {
            let client = match Self::connect_client(&account, config).await {
                Ok(client) => client,
                Err(e) if matches!(account.role, AccountRole::Buyer) => {
                    return Err(e).context(format!("Buyer {} account {:?} could not log in", account.id, account.phone));
                }
                Err(InvocationError::Rpc(rpc_err)) => {
                    if rpc_err.code == 401 || rpc_err.name == "FROZEN_METHOD_INVALID" || rpc_err.name == "AUTH_KEY_DUPLICATED" {
                        tracing::error!("CRITICAL: Client {} received fatal error {}: {}", account.id, rpc_err.code, rpc_err.name);
                        deactivate_account_from_db_and_send_alert(pg_pool, account.id, &account.phone, rpc_err.code, &notify_tx).await;
                    } else {
                        tracing::warn!("RPC error during invocation {}: {} for client {} with phone: {:?}",
                            rpc_err.code, rpc_err.name, account.id, account.phone);
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Other builder error for {}: {:?}", account.id, e);
                    continue;
                }
            };
            match account.role {
                AccountRole::Buyer => {
                    let input_peer = Self::generate_input_peer(&client, config)
                        .await
                        .context("Failed generate input peer")?;
                    tracing::info!("Buyer client {} with phone {:?} logged in", account.id, account.phone);
                    buyers.push((client, input_peer))
                },
                AccountRole::Viewer => {
                    tracing::info!("Viewer client {} with phone {:?} logged in", account.id, account.phone);
                    viewers.push(ViewerData { client, id: account.id, phone: account.phone })
                }
            }
            tokio::time::sleep(Duration::from_millis(65)).await;
        }

        if buyers.is_empty() {
            bail!("Critical error: No valid buyer accounts were initialized!");
        }
        if viewers.is_empty() {
            bail!("Critical error: No viewers initialized!");
        }

        tracing::info!("Buyers count - {}, Viewers count - {}", buyers.len(), viewers.len());

        let (new_viewers_tx, new_viewers_rx) = unbounded_channel();

        let acc_pool = AccountPool {
            buyers: Arc::from(buyers),
            viewers: ArcSwap::from_pointee(viewers),
            new_viewers_tx,
            notify_tx,
        };

        Ok((acc_pool, new_viewers_rx))
    }

    async fn connect_client(account: &Account, config: &AppConfig) -> Result<Client, InvocationError> {
        let config_client = Client::builder()
            .api_id(account.api_id.unwrap_or(config.default_api_id))
            .api_hash(account.api_hash.clone().unwrap_or(config.default_api_hash.clone()))
            .update_queue_capacity(0)
            .session_string(account.session.clone())
            .update_overflow_strategy(OverflowStrategy::DropNewest)
            .retry_policy(Arc::new(NoRetries))
            .transport(TransportKind::Abridged)
            .system_lang_code("en")
            .device_model("Android")
            .app_version("3.4.5")
            .system_version("16")
            .build().unwrap();

        let (client, _) = Client::connect(config_client).await?;
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

    pub async fn add_viewers_dynamic(&self, pg_pool: &PgPool, config: &AppConfig)
        -> Result<ViewersReport> {
        let active_in_memory_ids = self.viewers.load()
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>();

        let new_accounts = get_new_active_viewers(pg_pool, &active_in_memory_ids)
            .await
            .context("Failed to fetch dynamically added viewers from DB")?;

        let mut report = ViewersReport::default();

        if new_accounts.is_empty() { return Ok(report) }

        let mut new_viewers_data = Vec::with_capacity(new_accounts.len());

        for account in new_accounts {
            tracing::info!("Start of new client auth {} with phone: {:?}", account.id, account.phone);

            let client = match Self::connect_client(&account, config).await {
                Ok(client) => client,
                // Err(BuilderError::MissingApiId | BuilderError::MissingApiHash) => {
                //     tracing::warn!("Client {} with phone {:?} has invalid api id/hash", account.id, account.phone);
                //     report.failed.push((account.id, account.phone, "Invalid API ID/Hash".to_string()));
                //     continue;
                // }
                Err(InvocationError::Rpc(rpc_err)) => {
                    if rpc_err.code == 401 || rpc_err.name == "FROZEN_METHOD_INVALID" || rpc_err.name == "AUTH_KEY_DUPLICATED" {
                        tracing::error!("CRITICAL: Client {} received fatal error {}: {}", account.id, rpc_err.code, rpc_err.name);
                        deactivate_account_from_db_and_send_alert(pg_pool, account.id, &account.phone, rpc_err.code, &self.notify_tx).await;

                        report.failed.push((account.id, account.phone, format!("Banned/Frozen (Code: {})", rpc_err.code)));
                    } else {
                        tracing::warn!("RPC error during invocation {}: {}", rpc_err.code, rpc_err.name);
                        report.failed.push((account.id, account.phone, format!("RPC Error: {}", rpc_err.name)));
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Other builder error for {}: {:?}", account.id, e);
                    report.failed.push((account.id, account.phone, "Connection Timeout/Error".to_string()));
                    continue;
                }
            };

            let new_viewer_data = ViewerData {
                client,
                id: account.id,
                phone: account.phone.clone()
            };

            new_viewers_data.push(new_viewer_data.clone());

            if let Err(e) = self.new_viewers_tx.send(new_viewer_data) {
                bail!("MPSC channel closed with error: {e:?}");
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;

            report.added.push((account.id, account.phone));
        }

        self.viewers.rcu(|current_vec| {
            let mut next_vec = (**current_vec).clone();
            next_vec.extend(new_viewers_data.iter().cloned());
            next_vec
        });

        Ok(report)
    }

    pub async fn deactivate_account(&self,
                                    pg_pool: &PgPool,
                                    id: i32,
                                    phone: Option<String>,
                                    err_code: i32) {
        deactivate_account_from_db_and_send_alert(pg_pool,
                                                  id,
                                                  &phone,
                                                  err_code,
                                                  &self.notify_tx).await;
        tracing::info!("Client {id} with {phone:?} was deactivated with err_code {err_code}");

        let target_client = self.viewers.load()
            .iter()
            .find(|v| v.id == id)
            .map(|v| v.client.clone());

        if let Some(client) = target_client {
            self.viewers.rcu(|current_vec| {
                let mut next_vec = (**current_vec).clone();
                next_vec.retain(|v| v.id != id);
                next_vec
            });
            client.disconnect();
        }

    }

    pub async fn ping_update_status(&self) {
        let set_online_status = UpdateStatus { offline: false };
        let set_offline_status = UpdateStatus { offline: true };

        let mut report = ViewersReport::default();

        let viewers_copy = (**self.viewers.load()).clone();
        for viewer_data in viewers_copy {
            let viewer = viewer_data.client;
            if let Err(e) = viewer.invoke(&set_online_status).await {
                report.failed.push(
                    (viewer_data.id, viewer_data.phone, format!("Err when set online: {e:?}"))
                );
                continue;
            };
            tokio::time::sleep(Duration::from_millis(1000)).await;

            for attempt in 1..=2 {
                match viewer.invoke(&set_offline_status).await {
                    Ok(_) => break,
                    Err(e) => {
                        if attempt == 2 {
                            report.failed.push(
                                (viewer_data.id, viewer_data.phone.clone(),
                                 format!("Err when set offline: {e:?}"))
                            );
                            tracing::warn!("Error set offline for viewer: {:?}", viewer_data.phone);
                        } else {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        let mut msg = String::from("Update status finished");

        if !report.failed.is_empty() {
            let _ = writeln!(msg, "\n❌ Update status error for viewers: {}", report.failed.len());
            for (id, phone, reason) in report.failed {
                let _ = writeln!(msg, "ID {id} with phone {phone:?}: {reason}");
            }
        }

        let mut buyers_msg = String::new();
        for (buyer, _) in self.buyers.iter() {
            if let Err(e) = buyer.invoke(&set_online_status).await {
                let _ = writeln!(buyers_msg, "Error set online for buyer: {e:?}");
                tracing::warn!("Error set online for buyer: {e:?}");
                continue;
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
            for attempt in 1..=2 {
                match buyer.invoke(&set_offline_status).await {
                    Ok(_) => break,
                    Err(e) => {
                        if attempt == 2 {
                            let _ = writeln!(buyers_msg, "Error set offline for buyer: {e:?}");
                            tracing::warn!("Error set offline for buyer: {e:?}");
                        } else {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        if !buyers_msg.is_empty() {
            let _ = writeln!(msg, "\n❌ Update status error for buyers");
            let _ = writeln!(msg, "{buyers_msg}");
        }

        if let Err(e) = self.notify_tx.send(msg) {
            tracing::error!("Error when send msg by notify_tx: {e:?}");
        };
    }
}

async fn deactivate_account_from_db_and_send_alert(pg_pool: &PgPool,
                                                   id: i32,
                                                   phone: &Option<String>,
                                                   err_code: i32,
                                                   notify_tx: &UnboundedSender<String>) {
    let status = match err_code {
        401 => AccountStatus::Unauthorized,
        406 => AccountStatus::DuplicatedKey,
        _ => AccountStatus::Frozen
    };
    if let Err(e) = deactivate_account_by_id(pg_pool, id, status).await {
        tracing::warn!("Failed to deactivate account {id} in DB: {e:?}");
    }

    if let Err(e) = notify_tx.send(format!("Client {id} with {phone:?} was deactivated with err_code {err_code}")) {
        tracing::warn!("Channel alert is closed, alert doesn't sent: {e:?}");
    }
}