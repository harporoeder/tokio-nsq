extern crate rand;

use tokio_nsq::*;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

#[tokio::test]
async fn basic_consume_direct() {
    let rand_topic: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .collect();

    let topic   = NSQTopic::new(rand_topic).unwrap();
    let channel = NSQChannel::new("first").unwrap();

    let mut producer = NSQProducerConfig::new("127.0.0.1:4150").build();

    let mut addresses = std::vec::Vec::new();
    addresses.push("127.0.0.1:4150".to_string());

    let _ = producer.consume().await;
    producer.publish(&topic, "alice1".to_string().as_bytes().to_vec()).unwrap();

    let mut consumer = NSQConsumerConfig::new(topic, channel)
        .set_max_in_flight(1)
        .set_sources(NSQConsumerConfigSources::Daemons(addresses))
        .build();

    let message = consumer.consume_filtered().await.unwrap();

    assert_eq!(message.attempt, 1);
    assert_eq!(message.body, b"alice1");

    message.finish();
}
