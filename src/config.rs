use atrium_api::types::string::Did;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub chat_gpt_api_key: String,
    pub database_url: String,
    pub publisher_did: Did,
    pub feed_generator_hostname: String,
}
