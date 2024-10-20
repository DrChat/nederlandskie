use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use atrium_api::types::Collection;
use chrono::{DateTime, Utc};

use super::Algo;

use crate::services::database::{self, Database};

/// An algorithm that serves posts written in Russian by people living in Netherlands
pub struct Nederlandskie {
    _database: Arc<Database>,
}

impl Nederlandskie {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            _database: database,
        }
    }
}

impl Nederlandskie {}

#[async_trait]
impl Algo for Nederlandskie {
    async fn should_index_post(
        &self,
        _author_did: &str,
        _post: &<atrium_api::app::bsky::feed::Post as Collection>::Record,
    ) -> Result<bool> {
        todo!()
    }

    async fn fetch_posts(
        &self,
        _database: &Database,
        _limit: u8,
        _earlier_than: Option<(DateTime<Utc>, &str)>,
    ) -> Result<Vec<database::Post>> {
        todo!()
    }
}
