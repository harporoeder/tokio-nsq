use super::*;

use connection::*;
use connection_config::*;

/// Configuration object for an NSQ consumer
pub struct NSQProducerConfig {
    address: String,
    shared:  NSQConfigShared,
}

impl NSQProducerConfig {
    pub fn new(address: String) -> NSQProducerConfig {
        info!("NSQProducerConfig::new()");

        return NSQProducerConfig {
            address: address,
            shared:  NSQConfigShared::new(),
        }
    }

    pub fn set_shared(mut self, shared: NSQConfigShared) -> Self {
        self.shared = shared;

        return self;
    }

    pub fn build(self) -> NSQProducer {
        return NSQProducer {
            connection: NSQDConnection::new(NSQDConfig {
                address:   self.address,
                subscribe: None,
                shared:    self.shared,
            })
        }
    }
}

/// An NSQD producer corresponding to a single instance
pub struct NSQProducer {
    connection: NSQDConnection,
}

impl NSQProducer {
    pub async fn consume(&mut self) -> Option<NSQEvent> {
        return self.connection.consume().await;
    }

    pub fn publish(&mut self, topic: &Arc<NSQTopic>, value: Vec<u8>) {
        self.connection.publish(topic.clone(), value);
    }
}
