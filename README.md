# Tokio NSQ

![GitHub Actions](https://github.com/harporoeder/tokio-nsq/workflows/Rust/badge.svg)

A rust NSQ client built on Tokio. Tokio NSQ aims to be a feature complete NSQ client implementation.

## Versioning

This project follows strict semantic versioning. While pre `1.0.0` breaking changes have only a minor version bump.

## Basic consumer example

```rust
let mut addresses = HashSet::new();
addresses.insert("http://127.0.0.1:4161".to_string());

let mut consumer = NSQConsumerConfig::new(topic, channel)
    .set_max_in_flight(15)
    .set_sources(
        NSQConsumerConfigSources::Lookup(NSQConsumerLookupConfig {
            poll_interval: std::time::Duration::new(5, 0),
            addresses:     addresses,
        }
    ))
    .build();

let mut message = consumer.consume_filtered().await.unwrap();

let message_body_str = std::str::from_utf8(&message.body).unwrap();
println!("message body = {}", message_body_str);

message.finish();
```

## Features

- [x] Subscriptions
- [x] Publication
- [x] NSQLookupd based discovery.
- [ ] Message Backoff
- [x] NSQD TLS negotiation
- [ ] NSQD TLS client certificates
- [x] Deflate NSQD compression
- [ ] Snappy NSQD compression
- [ ] Sampling
- [ ] Auth
