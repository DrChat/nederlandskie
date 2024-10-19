use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use atrium_api::com::atproto::sync::subscribe_repos::Commit;
use atrium_api::types::Collection;
use chrono::{DateTime, Utc};
use tokio_stream::{Stream, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite};

use super::{
    entities::{FollowRecord, LikeRecord, PostRecord},
    internals::cbor::read_record,
    internals::ipld::Frame,
};

const ACTION_CREATE: &str = "create";
const ACTION_DELETE: &str = "delete";

pub const FIREHOSE_HOST: &'static str = "wss://bsky.network";
pub const STREAMING_TIMEOUT: Duration = Duration::from_secs(60);

pub struct CommitDetails {
    pub seq: i64,
    pub time: DateTime<Utc>,
    pub operations: Vec<Operation>,
}

#[derive(Debug)]
pub enum Operation {
    CreatePost {
        author_did: String,
        cid: String,
        uri: String,
        post: PostRecord,
    },
    CreateLike {
        author_did: String,
        cid: String,
        uri: String,
        like: LikeRecord,
    },
    CreateFollow {
        author_did: String,
        cid: String,
        uri: String,
        follow: FollowRecord,
    },
    DeletePost {
        uri: String,
    },
    DeleteLike {
        uri: String,
    },
    DeleteFollow {
        uri: String,
    },
}

/// Subscribe to the bluesky firehose.
pub async fn subscribe_to_operations(
    cursor: Option<i64>,
) -> Result<
    impl Stream<Item = Result<Result<tungstenite::Message, tungstenite::Error>, tokio_stream::Elapsed>>,
> {
    let url = match cursor {
        Some(cursor) => format!(
            "{}/xrpc/com.atproto.sync.subscribeRepos?cursor={}",
            FIREHOSE_HOST, cursor
        ),
        None => format!("{}/xrpc/com.atproto.sync.subscribeRepos", FIREHOSE_HOST),
    };

    let (stream, _) = connect_async(url).await?;
    let stream = stream.timeout(STREAMING_TIMEOUT);
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
                Ok(None)
            }
        }
        Frame::Message(None, _) => Ok(None),
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
                    Some(cid_link) => cid_link.0.to_string(),
                    None => continue,
                };

                let block = match blocks_by_cid.get(&cid) {
                    Some(block) => block,
                    None => continue,
                };

                match collection {
                    atrium_api::app::bsky::feed::Post::NSID => {
                        let post: PostRecord = read_record(block)?;

                        Operation::CreatePost {
                            author_did: commit.repo.to_string(),
                            cid: cid.to_string(),
                            uri,
                            post,
                        }
                    }
                    atrium_api::app::bsky::feed::Like::NSID => {
                        let like: LikeRecord = read_record(block)?;

                        Operation::CreateLike {
                            author_did: commit.repo.to_string(),
                            cid: cid.to_string(),
                            uri,
                            like,
                        }
                    }
                    atrium_api::app::bsky::graph::Follow::NSID => {
                        let follow: FollowRecord = read_record(block)?;

                        Operation::CreateFollow {
                            author_did: commit.repo.to_string(),
                            cid: cid.to_string(),
                            uri,
                            follow,
                        }
                    }
                    _ => continue,
                }
            }
            ACTION_DELETE => match collection {
                atrium_api::app::bsky::feed::Post::NSID => Operation::DeletePost { uri },
                atrium_api::app::bsky::feed::Like::NSID => Operation::DeleteLike { uri },
                atrium_api::app::bsky::graph::Follow::NSID => Operation::DeleteFollow { uri },
                _ => continue,
            },
            _ => continue,
        };

        operations.push(operation)
    }

    Ok(operations)
}
