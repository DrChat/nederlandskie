extern crate nederlandskie;

use std::sync::Arc;

use anyhow::{Context, Result};
use env_logger::Env;
use lingua::LanguageDetectorBuilder;
use log::info;

use nederlandskie::algos::{AlgosBuilder, Nederlandskie};
use nederlandskie::config::Config;
use nederlandskie::processes::{FeedServer, PostIndexer, ProfileClassifier};
use nederlandskie::services::{Bluesky, Database, AI};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Loading configuration");

    let config = Arc::new(Config::load().context("failed to load configuration")?);

    info!("Initializing service clients");

    let ai = Arc::new(AI::new(&config.chat_gpt_api_key, "https://api.openai.com"));
    let bluesky = Arc::new(Bluesky::unauthenticated());
    let database = Arc::new(
        Database::connect(&config.database_url)
            .await
            .context("failed to connect to database")?,
    );

    info!("Initializing language detector");

    let language_detector = Arc::new(
        LanguageDetectorBuilder::from_all_languages_with_cyrillic_script()
            .with_preloaded_language_models()
            .build(),
    );

    let algos = Arc::new(
        AlgosBuilder::new()
            .add(
                "nederlandskie",
                Nederlandskie::new(language_detector, database.clone()),
            )
            .build(),
    );

    let post_indexer = PostIndexer::new(
        database.clone(),
        bluesky.clone(),
        algos.clone(),
        config.clone(),
    );
    let profile_classifier = ProfileClassifier::new(database.clone(), ai.clone(), bluesky.clone());
    let feed_server = FeedServer::new(database.clone(), config.clone(), algos.clone());

    info!("Starting everything up");

    let _ = tokio::try_join!(
        tokio::spawn(post_indexer.start()),
        tokio::spawn(profile_classifier.start()),
        tokio::spawn(feed_server.serve()),
    )
    .context("failed to join tasks")?;

    Ok(())
}
