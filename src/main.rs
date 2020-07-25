extern crate simplelog;

use simplelog::*;
use std::collections::HashSet;

use nsq::*;

#[tokio::main]
async fn main() {
    let logging_config = ConfigBuilder::new()
        .add_filter_allow("nsq".to_string())
        .build();

    let _ = TermLogger::init(LevelFilter::Trace, logging_config, TerminalMode::Mixed);

    {
        let mut producer = NSQProducer::new(NSQProducerConfig{
            address: "127.0.0.1:4150".to_string(),
            tls:     Some(NSQDConfigTLS{}),
        });

        println!("waiting on status");

        let status = producer.consume().await;

        println!("status {:?}", status);

        producer.publish("names".to_string(), "alice1".to_string().as_bytes().to_vec());
        producer.publish("names".to_string(), "alice2".to_string().as_bytes().to_vec());
        producer.publish("names".to_string(), "alice3".to_string().as_bytes().to_vec());
        producer.publish("names".to_string(), "alice4".to_string().as_bytes().to_vec());
        producer.publish("names".to_string(), "alice5".to_string().as_bytes().to_vec());
    }

    let mut addresses = HashSet::new();
    addresses.insert("http://127.0.0.1:4161".to_string());

    let mut consumer = NSQConsumer::new(NSQConsumerConfig{
        topic:        "names".to_string(),
        channel:      "first".to_string(),
        tls:           Some(NSQDConfigTLS{}),
        max_in_flight: 15,
        sources: NSQConsumerConfigSources::Lookup(NSQConsumerLookupConfig {
            poll_interval: std::time::Duration::new(5, 0),
            addresses:     addresses,
        }),
    });

    loop {
        let mut message = consumer.consume_filtered().await.unwrap();

        message.touch();

        println!("message timestamp = {}", message.timestamp);
        println!("message attempt = {}", message.attempt);

        let message_id_str = std::str::from_utf8(&message.id).unwrap();
        println!("message id = {}", message_id_str);

        let message_body_str = std::str::from_utf8(&message.body).unwrap();
        println!("message body = {}", message_body_str);

        println!("consumed");

        message.finish();
    }
}
