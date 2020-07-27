use super::*;

use connection::*;
use connection_config::*;

/// Configuration object for an NSQ consumer
pub struct NSQProducerConfig {
    address: String,
    shared:  NSQConfigShared,
}

impl NSQProducerConfig {
    /// Construct a consumer with sane defaults.
    pub fn new<S: Into<String>>(address: S) -> NSQProducerConfig {
        return NSQProducerConfig {
            address: address.into(),
            shared:  NSQConfigShared::new(),
        }
    }

    /// NSQ Daemon connection options, such as compression and TLS.
    pub fn set_shared(mut self, shared: NSQConfigShared) -> Self {
        self.shared = shared;

        return self;
    }

    /// Construct an NSQ producer with this configuration.
    pub fn build(self) -> NSQProducer {
        return NSQProducer {
            connection: NSQDConnection::new(NSQDConfig {
                address:     self.address,
                subscribe:   None,
                shared:      self.shared,
                sample_rate: None,
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

    pub fn publish_deferred(&mut self, topic: &Arc<NSQTopic>, value: Vec<u8>, delay_seconds: u32) {
        self.connection.publish_deferred(topic.clone(), value, delay_seconds);
    }

    pub fn publish_multiple(&mut self, topic: &Arc<NSQTopic>, value: Vec<Vec<u8>>) {
        self.connection.publish_multiple(topic.clone(), value);
    }
}
