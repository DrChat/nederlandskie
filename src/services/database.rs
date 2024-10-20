use std::ops::Deref;

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};

pub struct Post {
    pub indexed_at: DateTime<Utc>,
    pub author_did: String,
    pub cid: String,
    pub uri: String,
}

pub struct Database {
    connection_pool: PgPool,
}

impl Deref for Database {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.connection_pool
    }
}

impl Database {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            connection_pool: PgPoolOptions::new().max_connections(5).connect(url).await?,
        })
    }

    pub async fn insert_post(&self, _author_did: &str, cid: &str, uri: &str) -> Result<()> {
        Ok(
            sqlx::query!("INSERT INTO Post (cid, uri) VALUES ($1, $2)", cid, uri)
                .execute(&self.connection_pool)
                .await
                .map(|_| ())?,
        )
    }

    pub async fn delete_post(&self, uri: &str) -> Result<bool> {
        Ok(sqlx::query!("DELETE FROM Post WHERE uri = $1", uri)
            .execute(&self.connection_pool)
            .await
            .map(|result| result.rows_affected() > 0)?)
    }

    pub async fn fetch_subscription_cursor(&self, host: &str, did: &str) -> Result<Option<i64>> {
        Ok(sqlx::query!(
            "SELECT cursor FROM SubscriptionState WHERE service = $1 AND host = $2",
            did,
            host
        )
        .map(|r| r.cursor.map(|i| i as i64))
        .fetch_optional(&self.connection_pool)
        .await?
        .flatten())
    }

    pub async fn create_subscription_state(&self, host: &str, did: &str) -> Result<bool> {
        Ok(sqlx::query!(
            "INSERT INTO SubscriptionState (service, cursor, host) VALUES ($1, $2, $3)",
            did,
            0,
            host
        )
        .execute(&self.connection_pool)
        .await
        .map(|result| result.rows_affected() > 0)?)
    }

    pub async fn update_subscription_cursor(
        &self,
        host: &str,
        did: &str,
        cursor: i64,
    ) -> Result<bool> {
        Ok(sqlx::query!(
            "UPDATE SubscriptionState SET cursor = $1 WHERE service = $2 AND host = $3",
            cursor as i32,
            did,
            host
        )
        .execute(&self.connection_pool)
        .await
        .map(|result| result.rows_affected() > 0)?)
    }
}
