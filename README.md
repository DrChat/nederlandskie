# `democraskie`

A Bluesky feed generator written in Rust.

This is a fork of despawnerer's [nederlandskie](https://github.com/despawnerer/nederlandskie), with an implementation of the algorithm described by [this research paper](https://arxiv.org/pdf/2307.13912).

Specifically, this feed catalogues political content but with an emphasis on positive partisanship rather than negative.

## Technical rundown

- Posts are stored in PostgreSQL via [`sqlx`](https://crates.io/crates/sqlx) and [`scooby`](https://crates.io/crates/scooby)
- Language of posts is determined via [`lingua-rs`](https://crates.io/crates/lingua)
- Country of residence is inferred from profile information through ChatGPT via [`chat-gpt-lib-rs`](https://crates.io/crates/chat-gpt-lib-rs)
- Feed is served via [`axum`](https://crates.io/crates/axum)
- Intefacing with Bluesky is implemented using [`atrium-api`](https://crates.io/crates/atrium-api)

## Setup

1. Copy `.env.example` into `.env` and set up the environment variables within:

   - `PUBLISHER_BLUESKY_HANDLE` to your Bluesky handle
   - `PUBLISHER_BLUESKY_PASSWORD` to Bluesky app password that you created in settings
   - `CHAT_GPT_API_KEY` for your ChatGPT key
   - `DATABASE_URL` for PostgreSQL credentials
   - `FEED_GENERATOR_HOSTNAME` to the hostname of where you intend to host the feed

2. Determine your own DID and put it in `PUBLISHER_DID` env variable in `.env`:

   ```
   cargo run --bin who_am_i
   ```

## Running

```
docker-compose up
```

### Development

```
docker-compose up -d db
cargo run
```

### Populate and serve the feed

`cargo run`

The feed will be available at http://localhost:3030/.

### Determine your own did for publishing

`cargo run --bin who_am_i`

### Publish the feed

`cargo run --bin publish_feed -- --help`

### Force a profile to be in a certain country

`cargo run --bin force_profile_country -- --help`

## Cross-compiling on non-Linux machines to deploy on Linux

1. Install `cross` by following their [installation guide](https://github.com/cross-rs/cross)

2. Build the binaries in release mode:

  ```
  cross build --release
  ```

3. Deploy the binaries in `target/x86_64-unknown-linux-gnu/release/` as you see fit
