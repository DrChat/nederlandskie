use anyhow::Result;
use clap::Parser;

use env_logger::Env;
use log::{error, info};
use nederlandskie::services::bluesky::{self, Operation};
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite;

#[derive(Parser, Debug)]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let _args = Args::parse();

    let mut stream = bluesky::subscribe_to_operations(None).await?;

    while let Some(tungstenite::Message::Binary(message)) = stream.try_next().await? {
        match bluesky::handle_message(&message).await {
            Ok(Some(commit)) => {
                for operation in commit.operations {
                    match operation {
                        Operation::CreatePost {
                            author_did: _author_did,
                            cid: _cid,
                            uri,
                            post,
                        } => {
                            if let Some(langs) = post.langs {
                                if langs.iter().any(|lang| lang == "en") {
                                    info!("{uri}: {}", post.text)
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(None) => continue,
            Err(e) => error!("Error handling a message: {:?}", e),
        }
    }

    Ok(())
}
