//! A Rust [NSQ](https://nsq.io/) client built on [Tokio](https://github.com/tokio-rs/tokio).
//! Tokio NSQ aims to be a feature complete NSQ client implementation.
//!
//! ## A basic consumer example:
//!```no_run
//! use tokio_nsq::*;
//! # #[tokio::main]
//! # async fn main() {
//!
//! let topic   = NSQTopic::new("names").unwrap();
//! let channel = NSQChannel::new("first").unwrap();
//!
//! let mut addresses = std::collections::HashSet::new();
//! addresses.insert("http://127.0.0.1:4161".to_string());
//!
//! let mut consumer = NSQConsumerConfig::new(topic, channel)
//!    .set_max_in_flight(15)
//!    .set_sources(
//!        NSQConsumerConfigSources::Lookup(
//!            NSQConsumerLookupConfig::new().set_addresses(addresses)
//!        )
//!    )
//!    .build();
//!
//! let mut message = consumer.consume_filtered().await.unwrap();
//!
//! let message_body_str = std::str::from_utf8(&message.body).unwrap();
//! println!("message body = {}", message_body_str);
//!
//! message.finish();
//! # }
//! ```

#![allow(dead_code)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate matches;

mod connection;
mod connection_config;
mod consumer;
mod producer;
mod snappy;
mod with_stopper;

pub use connection::{NSQEvent, NSQMessage, NSQRequeueDelay};

pub use producer::{NSQProducer, NSQProducerConfig};

pub use consumer::{
    NSQConsumer, NSQConsumerConfig, NSQConsumerConfigSources,
    NSQConsumerLookupConfig,
};

pub use connection_config::{
    NSQChannel, NSQConfigShared, NSQConfigSharedCompression,
    NSQConfigSharedTLS, NSQDeflateLevel, NSQSampleRate, NSQTopic,
};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
