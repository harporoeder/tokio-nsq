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

#[tokio::test]
async fn basic_consume_direct() {
    let topic   = random_topic();
    let channel = NSQChannel::new("test").unwrap();

    let producer = NSQProducerConfig::new("127.0.0.1:4150").build();

    let consumer = NSQConsumerConfig::new(topic.clone(), channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(vec!["127.0.0.1:4150".to_string()]))
        .build();

    cycle_messages(topic, producer, consumer).await;
}

#[tokio::test]
async fn basic_consume_lookup() {
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
async fn basic_direct_compression() {
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
