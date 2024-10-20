use anyhow::Result;
use chrono::{DateTime, Utc};
use scooby::postgres::{delete_from, insert_into, select, update, Parameters};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::query;
use sqlx::Row;

pub struct Post {
    pub indexed_at: DateTime<Utc>,
    pub author_did: String,
    pub cid: String,
    pub uri: String,
}

pub struct Database {
    connection_pool: PgPool,
}

impl Database {
    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self {
            connection_pool: PgPoolOptions::new().max_connections(5).connect(url).await?,
        })
    }

    pub async fn insert_post(&self, author_did: &str, cid: &str, uri: &str) -> Result<()> {
        let mut params = Parameters::new();

        Ok(query(
            &insert_into("Post")
                .columns(("author_did", "cid", "uri"))
                .values([params.next_array()])
                .to_string(),
        )
        .bind(author_did)
        .bind(cid)
        .bind(uri)
        .execute(&self.connection_pool)
        .await
        .map(|_| ())?)
    }

    pub async fn delete_post(&self, uri: &str) -> Result<bool> {
        let mut params = Parameters::new();

        Ok(query(
            &delete_from("Post")
                .where_(format!("uri = {}", params.next()))
                .to_string(),
        )
        .bind(uri)
        .execute(&self.connection_pool)
        .await
        .map(|result| result.rows_affected() > 0)?)
    }

    pub async fn fetch_subscription_cursor(&self, host: &str, did: &str) -> Result<Option<i64>> {
        let mut params = Parameters::new();

        Ok(query(
            &select("cursor")
                .from("SubscriptionState")
                .where_(format!("service = {}", params.next()))
                .where_(format!("host = {}", params.next()))
                .to_string(),
        )
        .bind(did)
        .bind(host)
        .map(|r: PgRow| r.get("cursor"))
        .fetch_optional(&self.connection_pool)
        .await?)
    }

    pub async fn create_subscription_state(&self, host: &str, did: &str) -> Result<bool> {
        let mut params = Parameters::new();

        Ok(query(
            &insert_into("SubscriptionState")
                .columns(("service", "cursor", "host"))
                .values([params.next_array()])
                .to_string(),
        )
        .bind(did)
        .bind(0)
        .bind(host)
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
        let mut params = Parameters::new();

        Ok(query(
            &update("SubscriptionState")
                .set("cursor", params.next())
                .where_(format!("service = {}", params.next()))
                .where_(format!("host = {}", params.next()))
                .to_string(),
        )
        .bind(cursor)
        .bind(did)
        .bind(host)
        .execute(&self.connection_pool)
        .await
        .map(|result| result.rows_affected() > 0)?)
    }
}
