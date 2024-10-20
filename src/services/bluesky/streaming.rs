use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use atrium_api::{com::atproto::sync::subscribe_repos::Commit, types::string::Did};
use chrono::{DateTime, Utc};
use ipld_core::cid::Cid;
use tokio_stream::Stream;
use tokio_tungstenite::{connect_async, tungstenite};

use super::internals::ipld::Frame;

const ACTION_CREATE: &str = "create";
const ACTION_DELETE: &str = "delete";

pub const FIREHOSE_HOST: &'static str = "wss://bsky.network";
pub const STREAMING_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct CommitDetails {
    pub seq: i64,
    pub time: DateTime<Utc>,
    pub operations: Vec<Operation>,
}

#[derive(Debug, Clone)]
pub enum Operation {
    Create {
        collection: String,
        did: Did,
        cid: Cid,
        block: Vec<u8>,
    },
    Delete {
        uri: String,
    },
}

/// Subscribe to the bluesky firehose.
pub async fn subscribe_to_operations(
    cursor: Option<i64>,
) -> Result<impl Stream<Item = Result<tungstenite::Message, tungstenite::Error>>> {
    let url = match cursor {
        Some(cursor) => format!(
            "{}/xrpc/com.atproto.sync.subscribeRepos?cursor={}",
            FIREHOSE_HOST, cursor
        ),
        None => format!("{}/xrpc/com.atproto.sync.subscribeRepos", FIREHOSE_HOST),
    };

    let (stream, _) = connect_async(url).await?;
    let stream = Box::pin(stream);

    Ok(stream)
}

pub async fn handle_message(message: &[u8]) -> Result<Option<CommitDetails>> {
    let commit = match parse_commit_from_message(message)? {
        Some(commit) => commit,
        None => return Ok(None),
    };

    let operations = extract_operations(&commit).await?;

    Ok(Some(CommitDetails {
        seq: commit.seq,
        time: (*commit.time.as_ref()).into(),
        operations,
    }))
}

fn parse_commit_from_message(message: &[u8]) -> Result<Option<Commit>> {
    match Frame::try_from(message)? {
        Frame::Message(Some(t), message) => {
            if t == "#commit" {
                Ok(serde_ipld_dagcbor::from_reader(message.body.as_slice())?)
            } else {
                // Spec: "Clients should ignore frames with headers that have unknown `op` or `t` values"
                Ok(None)
            }
        }
        Frame::Message(None, _) => Ok(None),
        // Spec: "Streams should be closed immediately following transmitting or receiving an error frame."
        Frame::Error(err) => panic!("Frame error: {err:?}"),
    }
}

async fn extract_operations(commit: &Commit) -> Result<Vec<Operation>> {
    let mut operations = Vec::new();

    let (blocks, _header) = rs_car::car_read_all(&mut commit.blocks.as_slice(), true).await?;
    let blocks_by_cid: HashMap<_, _> = blocks
        .into_iter()
        .map(|(cid, block)| (cid.to_string(), block))
        .collect();

    for op in &commit.ops {
        let collection = op.path.split('/').next().expect("op.path is empty");
        let action = op.action.as_str();
        let uri = format!("at://{}/{}", commit.repo.as_str(), op.path);

        let operation = match action {
            ACTION_CREATE => {
                let cid = match &op.cid {
                    Some(cid_link) => cid_link.0,
                    None => continue,
                };

                let block = match blocks_by_cid.get(&cid.to_string()) {
                    Some(block) => block,
                    None => continue,
                };

                Operation::Create {
                    collection: collection.to_string(),
                    did: commit.repo.clone(),
                    cid: cid,
                    block: block.to_vec(),
                }
            }
            ACTION_DELETE => Operation::Delete { uri },
            _ => continue,
        };

        operations.push(operation)
    }

    Ok(operations)
}
