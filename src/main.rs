extern crate nederlandskie;

use std::sync::Arc;

use anyhow::{Context, Result};
use env_logger::Env;
use log::info;

use nederlandskie::algos::{AlgosBuilder, Nederlandskie};
use nederlandskie::config::Config;
use nederlandskie::processes::{feed_server, post_indexer};
use nederlandskie::services::{Bluesky, Database};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Loading configuration");

    let config = Arc::new(Config::load().context("failed to load configuration")?);

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

    info!("Starting everything up");

    let _ = tokio::try_join!(
        tokio::spawn(post_indexer::start(
            database.clone(),
            config.clone(),
            algos.clone(),
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
