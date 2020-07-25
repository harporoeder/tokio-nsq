use super::*;

use connection::*;

pub struct NSQProducerConfig {
    pub address: String
}

pub struct NSQProducer {
    connection: NSQDConnection
}

impl NSQProducer {
    pub fn new(config: NSQProducerConfig) -> NSQProducer {
        info!("NSQProducer::new()");

        return NSQProducer {
            connection: NSQDConnection::new(NSQDConfig {
                address:   config.address,
                subscribe: None,
                tls:       None,
            })
        }
    }

    pub async fn consume(&mut self) -> Option<NSQEvent> {
        return self.connection.consume().await;
    }

    pub fn publish(&mut self, topic: String, value: Vec<u8>) {
        self.connection.publish(topic, value);
    }
}
