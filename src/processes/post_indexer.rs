use std::sync::Arc;

use anyhow::Result;
use atrium_api::types::Collection;
use log::{debug, error, info};
use tokio::sync::broadcast;

use crate::algos::Algos;
use crate::config::Config;
use crate::services::bluesky::{CommitDetails, Operation, FIREHOSE_HOST};
use crate::services::Database;

pub async fn start(
    database: Arc<Database>,
    config: Arc<Config>,
    algos: Arc<Algos>,
    mut firehose: broadcast::Receiver<CommitDetails>,
) -> Result<()> {
    while let Ok(commit) = firehose.recv().await {
        process_commit(&database, &algos, &config, &commit).await?;
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
