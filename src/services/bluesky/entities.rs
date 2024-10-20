mod profile;

pub use profile::ProfileDetails;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FollowRecord {
    pub subject: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LikeRecord {
    pub subject: Subject,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Subject {
    pub cid: String,
    pub uri: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PostRecord {
    pub text: String,
    pub langs: Option<Vec<String>>,
    pub reply: Option<ReplyRef>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReplyRef {
    pub parent: Ref,
    pub root: Ref,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Ref {
    pub cid: String,
    pub uri: String,
}
