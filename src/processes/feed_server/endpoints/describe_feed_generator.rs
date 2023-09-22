use atrium_api::app::bsky::feed::describe_feed_generator::{
    Feed, Output as FeedGeneratorDescription,
};
use axum::{extract::State, Json};

use crate::processes::feed_server::state::FeedServerState;

pub async fn describe_feed_generator(
    State(state): State<FeedServerState>,
) -> Json<FeedGeneratorDescription> {
    Json(FeedGeneratorDescription {
        did: state.config.feed_generator_did.clone(),
        feeds: state
            .algos
            .iter_names()
            .map(|name| Feed {
                uri: format!(
                    "at://{}/app.bsky.feed.generator/{}",
                    state.config.publisher_did, name
                ),
            })
            .collect(),
        links: None,
    })
}
