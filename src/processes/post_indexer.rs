use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use atrium_api::types::Collection;
use log::{debug, error, info};
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite;

use crate::algos::Algos;
use crate::config::Config;
use crate::services::bluesky::{self, CommitDetails, Operation, FIREHOSE_HOST};
use crate::services::Database;

pub async fn start(database: Arc<Database>, config: Arc<Config>, algos: Arc<Algos>) -> Result<()> {
    info!("Starting");

    loop {
        if let Err(e) = process_from_last_point(&database, &algos, &config).await {
            error!("Stopped because of an error: {}", e);
        }

        info!("Waiting 10 seconds before reconnecting...");

        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

async fn process_from_last_point(
    database: &Database,
    algos: &Algos,
    config: &Config,
) -> Result<()> {
    let cursor = database
        .fetch_subscription_cursor(FIREHOSE_HOST, &config.feed_generator_did)
        .await?;

    if cursor.is_none() {
        database
            .create_subscription_state(FIREHOSE_HOST, &config.feed_generator_did)
            .await?;
    }

    info!("Subscribing with cursor {:?}", cursor);

    let mut stream = pin!(bluesky::subscribe_to_operations(cursor)
        .await
        .context("failed to subscribe")?
        .timeout(bluesky::STREAMING_TIMEOUT));

    while let Some(Ok(tungstenite::Message::Binary(message))) = stream.try_next().await? {
        match bluesky::handle_message(&message).await {
            Ok(Some(commit)) => process_commit(database, algos, config, &commit).await?,
            Ok(None) => continue,
            Err(e) => error!("Error handling a message: {:?}", e),
        }
    }

    Ok(())
}

async fn process_commit(
    database: &Database,
    algos: &Algos,
    config: &Config,
    commit: &CommitDetails,
) -> Result<()> {
    for operation in &commit.operations {
        match operation {
            Operation::Create {
                collection,
                did,
                cid,
                block,
            } => {
                let uri = format!("at://{}/{}/{}", did.as_str(), collection, cid);

                if collection == atrium_api::app::bsky::feed::Post::NSID {
                    let post = match serde_ipld_dagcbor::from_slice::<
                        <atrium_api::app::bsky::feed::Post as Collection>::Record,
                    >(&block[..])
                    {
                        Ok(post) => post,
                        Err(e) => {
                            error!("Error deserializing a post: {:?}", e,);
                            continue;
                        }
                    };

                    for algo in algos.iter_all() {
                        if algo.should_index_post(did, &post).await? {
                            info!("Received insertable post from {}: {post:?}", did.as_str());

                            database.insert_post(did, &cid.to_string(), &uri).await?;

                            break;
                        }
                    }
                }
            }
            Operation::Delete { uri: _uri } => {
                // info!("Received a post to delete: {uri}");
                // self.database.delete_post(uri).await?;
            }
        }
    }

    if commit.seq % 20 == 0 {
        debug!(
            "Updating cursor for {} to {} ({})",
            config.feed_generator_did.as_str(),
            commit.seq,
            commit.time
        );
        database
            .update_subscription_cursor(FIREHOSE_HOST, &config.feed_generator_did, commit.seq)
            .await?;
    }

    Ok(())
}
