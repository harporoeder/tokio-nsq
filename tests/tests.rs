extern crate rand;
#[macro_use]
extern crate matches;

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

async fn cycle_messages(
    topic: Arc<NSQTopic>, mut producer: NSQProducer, mut consumer: NSQConsumer
) {
    let n: u8 = 10;

    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());

    for x in 0..n {
        producer.publish(&topic, x.to_string().as_bytes().to_vec()).unwrap();
    };

    for _ in 0..n {
        assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());
    }

    for x in 0..n {
        let message = consumer.consume_filtered().await.unwrap();

        assert_eq!(message.attempt, 1);
        assert_eq!(message.body, x.to_string().as_bytes().to_vec());

        message.finish();
    }
}

fn make_default() -> (Arc<NSQTopic>, NSQProducer, NSQConsumer) {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("127.0.0.1:4150")
        .set_shared(
            NSQConfigShared::new().set_compression(
                NSQConfigSharedCompression::Deflate(NSQDeflateLevel::new(3).unwrap())
            )
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec!["127.0.0.1:4150".to_string()]))
        .set_shared(
            NSQConfigShared::new().set_compression(
                NSQConfigSharedCompression::Deflate(NSQDeflateLevel::new(3).unwrap())
            )
        )
        .build();

    (topic, producer, consumer)
}

#[tokio::test]
async fn direct_connection_basic() {
    let (topic, producer, consumer) = make_default();

    cycle_messages(topic, producer, consumer).await;
}

#[tokio::test]
async fn lookup_consume_basic() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("127.0.0.1:4150").build();

    let mut addresses = HashSet::new();
    addresses.insert("http://127.0.0.1:4161".to_string());

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(
            NSQConsumerConfigSources::Lookup(
                NSQConsumerLookupConfig::new()
                    .set_addresses(addresses)
                    .set_poll_interval(std::time::Duration::from_millis(10))
            )
        )
        .build();

    cycle_messages(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_conection_compression() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("127.0.0.1:4150")
        .set_shared(
            NSQConfigShared::new().set_compression(
                NSQConfigSharedCompression::Deflate(NSQDeflateLevel::new(3).unwrap())
            )
        )
        .build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec!["127.0.0.1:4150".to_string()]))
        .set_shared(
            NSQConfigShared::new().set_compression(
                NSQConfigSharedCompression::Deflate(NSQDeflateLevel::new(3).unwrap())
            )
        )
        .build();

    cycle_messages(topic, producer, consumer).await;
}

#[tokio::test]
async fn direct_connection_dpub() {
    let (topic, mut producer, mut consumer) = make_default();

    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());
    producer.publish_deferred(&topic, b"hello".to_vec(), 1).unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();

    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"hello".to_vec());

    message.finish();
}

#[tokio::test]
async fn direct_connection_mpub() {
    let (topic, mut producer, mut consumer) = make_default();

    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Healthy());
    producer.publish_multiple(&topic, vec![b"hello1".to_vec(), b"hello2".to_vec()]).unwrap();
    assert_matches!(producer.consume().await.unwrap(), NSQEvent::Ok());

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"hello1".to_vec());
    message.finish();

    let message = consumer.consume_filtered().await.unwrap();
    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"hello2".to_vec());
    message.finish();
}
