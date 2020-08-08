extern crate rand;

use tokio_nsq::*;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use std::collections::HashSet;
use std::sync::Arc;

fn random_topic() -> Arc<NSQTopic> {
    let name: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .collect();

    NSQTopic::new(name).unwrap()
}

#[tokio::test]
async fn basic_consume_direct() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let mut producer = NSQProducerConfig::new("127.0.0.1:4150").build();

    let mut addresses = std::vec::Vec::new();
    addresses.push("127.0.0.1:4150".to_string());

    let _ = producer.consume().await;
    producer.publish(&topic, b"alice1".to_vec()).unwrap();

    let mut consumer = NSQConsumerConfig::new(topic, channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(addresses))
        .build();

    let message = consumer.consume_filtered().await.unwrap();

    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"alice1");

    message.finish();
}

#[tokio::test]
async fn basic_consume_lookup() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let mut producer = NSQProducerConfig::new("127.0.0.1:4150").build();

    let mut addresses = std::vec::Vec::new();
    addresses.push("127.0.0.1:4150".to_string());

    let _ = producer.consume().await;
    producer.publish(&topic, b"alice2".to_vec()).unwrap();

    let mut addresses = HashSet::new();
    addresses.insert("http://127.0.0.1:4161".to_string());

    let mut consumer = NSQConsumerConfig::new(topic, channel)
        .set_max_in_flight(1)
        .set_sources(
            NSQConsumerConfigSources::Lookup(
                NSQConsumerLookupConfig::new()
                    .set_addresses(addresses)
                    .set_poll_interval(std::time::Duration::from_millis(10))
            )
        )
        .build();

    let message = consumer.consume_filtered().await.unwrap();

    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"alice2");

    message.finish();
}

#[tokio::test]
async fn basic_direct_compression() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let mut producer = NSQProducerConfig::new("127.0.0.1:4150")
        .set_shared(
            NSQConfigShared::new().set_compression(NSQConfigSharedCompression::Deflate(3))
        )
        .build();

    let mut addresses = std::vec::Vec::new();
    addresses.push("127.0.0.1:4150".to_string());

    let _ = producer.consume().await;
    producer.publish(&topic, b"alice3".to_vec()).unwrap();

    let mut consumer = NSQConsumerConfig::new(topic, channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(addresses))
        .set_shared(
            NSQConfigShared::new().set_compression(NSQConfigSharedCompression::Deflate(3))
        )
        .build();

    let message = consumer.consume_filtered().await.unwrap();

    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"alice3");

    message.finish();
}
