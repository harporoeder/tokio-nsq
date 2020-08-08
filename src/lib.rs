//! A Rust [NSQ](https://nsq.io/) client built on [Tokio](https://github.com/tokio-rs/tokio).
//! Tokio NSQ aims to be a feature complete NSQ client implementation.
//!
//! ## A basic consumer example:
//!```no_run
//!use tokio_nsq::*;
//! # #[tokio::main]
//! # async fn main() {
//!
//!let topic   = NSQTopic::new("names").unwrap();
//!let channel = NSQChannel::new("first").unwrap();
//!
//!let mut addresses = std::collections::HashSet::new();
//!addresses.insert("http://127.0.0.1:4161".to_string());
//!
//!let mut consumer = NSQConsumerConfig::new(topic, channel)
//!    .set_max_in_flight(15)
//!    .set_sources(
//!        NSQConsumerConfigSources::Lookup(
//!            NSQConsumerLookupConfig::new().set_addresses(addresses)
//!        )
//!    )
//!    .build();
//!
//!let mut message = consumer.consume_filtered().await.unwrap();
//!
//!let message_body_str = std::str::from_utf8(&message.body).unwrap();
//!println!("message body = {}", message_body_str);
//!
//!message.finish();
//! # }
//!```

#![allow(dead_code)]

extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate byteorder;
extern crate log;
extern crate tokio_rustls;
extern crate rustls;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate backoff;
extern crate miniz_oxide;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use log::*;
use crate::tokio::io::AsyncWrite;
use crate::tokio::io::AsyncRead;
use crate::tokio::io::AsyncWriteExt;
use crate::tokio::io::AsyncReadExt;
use std::convert::TryFrom;
use failure::{Error};

mod connection;
mod producer;
mod consumer;
mod compression;
mod connection_config;

pub use connection::
    { NSQTopic
    , NSQChannel
    , NSQEvent
    , NSQMessage
    };

pub use producer::
    { NSQProducerConfig
    , NSQProducer
    };

pub use consumer::
    { NSQConsumerConfig
    , NSQConsumerConfigSources
    , NSQConsumerLookupConfig
    , NSQConsumer
    };

pub use connection_config::
    { NSQConfigSharedTLS
    , NSQConfigSharedCompression
    , NSQConfigShared
    , NSQDeflateLevel
    };
