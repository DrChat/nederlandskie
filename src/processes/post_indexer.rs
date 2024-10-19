use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use log::{debug, error, info};
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite;

use crate::algos::Algos;
use crate::config::Config;
use crate::services::bluesky::{self, CommitDetails, Operation, FIREHOSE_HOST};
use crate::services::Database;

pub struct PostIndexer {
    database: Arc<Database>,
    algos: Arc<Algos>,
    config: Arc<Config>,
}

impl PostIndexer {
    pub fn new(database: Arc<Database>, algos: Arc<Algos>, config: Arc<Config>) -> Self {
        Self {
            database,
            algos,
            config,
        }
    }
}

impl PostIndexer {
    pub async fn start(self) -> Result<()> {
        info!("Starting");

        loop {
            if let Err(e) = self.process_from_last_point().await {
                error!("Stopped because of an error: {}", e);
            }

            info!("Waiting 10 seconds before reconnecting...");

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn process_from_last_point(&self) -> Result<()> {
        let cursor = self
            .database
            .fetch_subscription_cursor(FIREHOSE_HOST, &self.config.feed_generator_did)
            .await?;

        if cursor.is_none() {
            self.database
                .create_subscription_state(FIREHOSE_HOST, &self.config.feed_generator_did)
                .await?;
        }

        info!("Subscribing with cursor {:?}", cursor);

        let mut stream = pin!(bluesky::subscribe_to_operations(cursor)
            .await
            .context("failed to subscribe")?
            .timeout(bluesky::STREAMING_TIMEOUT));

        while let Some(Ok(tungstenite::Message::Binary(message))) = stream.try_next().await? {
            match bluesky::handle_message(&message).await {
                Ok(Some(commit)) => self.process_commit(&commit).await?,
                Ok(None) => continue,
                Err(e) => error!("Error handling a message: {:?}", e),
            }
        }

        Ok(())
    }

    async fn process_commit(&self, commit: &CommitDetails) -> Result<()> {
        for operation in &commit.operations {
            match operation {
                Operation::CreatePost {
                    author_did,
                    cid,
                    uri,
                    post,
                } => {
                    for algo in self.algos.iter_all() {
                        if algo.should_index_post(author_did, post).await? {
                            info!("Received insertable post from {author_did}: {post:?}",);

                            self.database
                                .insert_profile_if_it_doesnt_exist(author_did)
                                .await?;

                            self.database.insert_post(author_did, cid, uri).await?;

                            break;
                        }
                    }
                }
                Operation::DeletePost { uri } => {
                    info!("Received a post to delete: {uri}");

                    self.database.delete_post(uri).await?;
                }
                _ => continue,
            }
        }

        if commit.seq % 20 == 0 {
            debug!(
                "Updating cursor for {} to {} ({})",
                self.config.feed_generator_did.as_str(),
                commit.seq,
                commit.time
            );
            self.database
                .update_subscription_cursor(
                    FIREHOSE_HOST,
                    &self.config.feed_generator_did,
                    commit.seq,
                )
                .await?;
        }

        Ok(())
    }
}
