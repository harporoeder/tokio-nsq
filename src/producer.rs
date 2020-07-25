use super::*;

use connection::*;

pub struct NSQProducerConfig {
    pub address: String,
    pub tls:     Option<NSQDConfigTLS>,
}

pub struct NSQProducer {
    connection: NSQDConnection,
}

impl NSQProducer {
    pub fn new(config: NSQProducerConfig) -> NSQProducer {
        info!("NSQProducer::new()");

        return NSQProducer {
            connection: NSQDConnection::new(NSQDConfig {
                address:   config.address,
                subscribe: None,
                tls:       config.tls,
            })
        }
    }

    pub async fn consume(&mut self) -> Option<NSQEvent> {
        return self.connection.consume().await;
    }

    pub fn publish(&mut self, topic: &Arc<NSQTopic>, value: Vec<u8>) {
        self.connection.publish(topic.clone(), value);
    }
}
