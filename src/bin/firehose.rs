use anyhow::Result;
use clap::Parser;

use env_logger::Env;
use log::{error, info};
use nederlandskie::services::bluesky;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite;

#[derive(Parser, Debug)]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let _args = Args::parse();

    let mut stream = bluesky::subscribe_to_operations(None).await?;

    while let Some(Ok(tungstenite::Message::Binary(message))) = stream.try_next().await? {
        match bluesky::handle_message(&message).await {
            Ok(Some(commit)) => info!("{:?}", commit),
            Ok(None) => continue,
            Err(e) => error!("Error handling a message: {:?}", e),
        }
    }

    Ok(())
}
