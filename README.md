# Tokio NSQ

![GitHub Actions](https://github.com/harporoeder/tokio-nsq/workflows/Rust/badge.svg)
![crates.io](https://img.shields.io/crates/v/tokio-nsq.svg)

A Rust [NSQ](https://nsq.io/) client built on [Tokio](https://github.com/tokio-rs/tokio). Tokio NSQ aims to be a feature complete NSQ client implementation.

Tokio NSQ is available as a [cargo package](https://crates.io/crates/tokio-nsq), and API documentation is available on [docs.rs](https://docs.rs/tokio-nsq/latest/tokio_nsq/).

## Versioning

This project follows strict semantic versioning. While pre `1.0.0` breaking changes have only a minor version bump.

## Basic consumer example

```rust
let topic   = NSQTopic::new("names").unwrap();
let channel = NSQChannel::new("first").unwrap();

let mut addresses = HashSet::new();
addresses.insert("http://127.0.0.1:4161".to_string());

let mut consumer = NSQConsumerConfig::new(topic, channel)
    .set_max_in_flight(15)
    .set_sources(
        NSQConsumerConfigSources::Lookup(
            NSQConsumerLookupConfig::new().set_addresses(addresses)
        )
    )
    .build();

let mut message = consumer.consume_filtered().await.unwrap();

let message_body_str = std::str::from_utf8(&message.body).unwrap();
println!("message body = {}", message_body_str);

message.finish();
```

## Basic producer example

```rust
let topic = NSQTopic::new("names").unwrap();

let mut producer = NSQProducerConfig::new("127.0.0.1:4150").build();

// Wait until a connection is initialized
assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());
// Publish a single message
producer.publish(&topic, b"alice1".to_vec()).unwrap();
// Wait until the message is acknowledged by NSQ
assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());
```

## Features

- [x] Subscriptions
- [x] Publication
- [x] NSQLookupd based discovery.
- [x] Message requeue backoff
- [X] NSQD TLS negotiation
    - [x] Unverified server certificates
    - [X] Custom certificate authority
    - [X] Client certificates
- [x] Deflate NSQD compression
- [X] Snappy NSQD compression
- [X] Sampling
- [X] Auth
