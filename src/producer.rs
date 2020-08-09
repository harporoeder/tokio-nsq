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
        NSQProducerConfig {
            address: address.into(),
            shared:  NSQConfigShared::new(),
        }
    }

    /// NSQ Daemon connection options, such as compression and TLS.
    pub fn set_shared(mut self, shared: NSQConfigShared) -> Self {
        self.shared = shared;

        self
    }

    /// Construct an NSQ producer with this configuration.
    pub fn build(self) -> NSQProducer {
        NSQProducer {
            connection: NSQDConnection::new(NSQDConfig {
                address:            self.address,
                subscribe:          None,
                shared:             self.shared,
                sample_rate:        None,
                max_requeue_delay:  std::time::Duration::from_secs(60 * 15),
                base_requeue_delay: std::time::Duration::from_secs(90),
            })
        }
    }
}

/// An NSQD producer corresponding to a single instance.
///
/// Before any messages are published you must wait for an `NSQEvent::Healthy()` message.
/// If any messages are queued while the connection is unhealthy the publish method shall return
/// an error and the message will not be queued. Messages are queued and delivered asynchronously.
/// Once NSQ acknowledges a message an `NSQEvent::Ok()` will be available from `consume`.
///
/// Multiple messages can be queued before any are acknowledged. You do not need to wait on
/// `consume` immediately after each publish. If any `NSQEvent::Unhealthy()` event is ever
/// returned, all unacknowledged messages up to that point are now considered failed, and must be
/// requeued. A producer will not buffer messages waiting for a healthy connection.
pub struct NSQProducer {
    connection: NSQDConnection,
}

impl NSQProducer {
    /// Consume message acknowledgements, and connection status updates.
    pub async fn consume(&mut self) -> Option<NSQEvent> {
        self.connection.consume().await
    }

    /// Queue a PUB message to be asynchronously sent
    pub fn publish(&mut self, topic: &Arc<NSQTopic>, value: Vec<u8>) -> Result<(), Error> {
        self.connection.publish(topic.clone(), value)
    }

    /// Queue a DPUB message to be asynchronously sent
    pub fn publish_deferred(
        &mut self, topic: &Arc<NSQTopic>, value: Vec<u8>, delay_milliseconds: u32
    ) -> Result<(), Error>
    {
        self.connection.publish_deferred(topic.clone(), value, delay_milliseconds)
    }

    /// Queue an MPUB message to be asynchronously sent
    pub fn publish_multiple(&mut self, topic: &Arc<NSQTopic>, value: Vec<Vec<u8>>)
        -> Result<(), Error>
    {
        self.connection.publish_multiple(topic.clone(), value)
    }
}
