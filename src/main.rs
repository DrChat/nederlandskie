extern crate nederlandskie;

use std::pin::pin;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use log::info;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite;

use nederlandskie::algos::{AlgosBuilder, Nederlandskie};
use nederlandskie::config::Config;
use nederlandskie::processes::{feed_server, post_indexer};
use nederlandskie::services::bluesky::{CommitDetails, FIREHOSE_HOST};
use nederlandskie::services::{bluesky, Bluesky, Database};

/// This is the primary point where messages are consumed from the BlueSky network.
///
/// Once ingested and validated, the message is then forwarded out to the broadcast
/// channel via `tx`.
async fn firehose_server(cursor: Option<i64>, tx: broadcast::Sender<CommitDetails>) -> Result<()> {
    let mut stream = pin!(bluesky::subscribe_to_operations(cursor)
        .await
        .context("failed to subscribe")?
        .timeout(bluesky::STREAMING_TIMEOUT));

    while let Some(Ok(tungstenite::Message::Binary(message))) = stream.try_next().await? {
        match bluesky::handle_message(&message).await {
            Ok(Some(commit)) => {
                tx.send(commit)?;
            }
            Ok(None) => continue,
            Err(e) => bail!("Error handling a message: {:?}", e),
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("Loading configuration");

    let config = Arc::new(
        Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Env::prefixed("BSKY_"))
            .extract::<Config>()
            .context("failed to load configuration")?,
    );

    info!("Initializing service clients");

    let _bluesky = Arc::new(Bluesky::unauthenticated());
    let database = Arc::new(
        Database::connect(&config.database_url)
            .await
            .context("failed to connect to database")?,
    );

    let algos = Arc::new(
        AlgosBuilder::new()
            .add("nederlandskie", Nederlandskie::new(database.clone()))
            .build(),
    );

    let cursor = database
        .fetch_subscription_cursor(FIREHOSE_HOST, &config.feed_generator_hostname)
        .await?;

    if cursor.is_none() {
        database
            .create_subscription_state(FIREHOSE_HOST, &config.feed_generator_hostname)
            .await?;
    }

    let (tx, rx) = broadcast::channel(10);

    info!("Starting everything up");

    let _ = tokio::try_join!(
        tokio::spawn(firehose_server(cursor, tx)),
        tokio::spawn(post_indexer::start(
            database.clone(),
            config.clone(),
            algos.clone(),
            rx
        )),
        tokio::spawn(feed_server::serve(
            database.clone(),
            config.clone(),
            algos.clone()
        )),
    )
    .context("failed to join tasks")?;

    Ok(())
}
