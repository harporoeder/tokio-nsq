use ::core::result::Result;
use ::failure::Error;
use ::log::*;
use ::std::collections::HashMap;
use ::std::collections::HashSet;
use ::std::sync::Arc;

use crate::connection::*;
use crate::connection_config::*;
use crate::with_stopper::with_stopper;

/// Configuration object for NSQLookup nodes
#[derive(Clone)]
pub struct NSQConsumerLookupConfig {
    poll_interval: std::time::Duration,
    addresses: HashSet<String>,
}

impl NSQConsumerLookupConfig {
    /// Construct a Lookup Daemon configuration. Defaults to no connections.
    pub fn new() -> Self {
        NSQConsumerLookupConfig {
            poll_interval: std::time::Duration::new(60, 0),
            addresses: HashSet::new(),
        }
    }

    /// How often an NSQD Lookup Daemon instance should be queried. Defaults to
    /// every 60 seconds.
    pub fn set_poll_interval(
        mut self,
        poll_interval: std::time::Duration,
    ) -> Self {
        self.poll_interval = poll_interval;

        self
    }

    /// The set of HTTP addresses for NSQ Lookup Daemon connections. Defaults to
    /// no connections.
    pub fn set_addresses(mut self, addresses: HashSet<String>) -> Self {
        self.addresses = addresses;

        self
    }
}

impl Default for NSQConsumerLookupConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The strategy a consumer should use to connect to NSQD instances
#[derive(Clone)]
pub enum NSQConsumerConfigSources {
    /// An explicit list of NSQD daemons to use.
    Daemons(Vec<String>),
    /// Configuration for NSQD Lookup daemons.
    Lookup(NSQConsumerLookupConfig),
}

/// Configuration object for an NSQ consumer.
#[derive(Clone)]
pub struct NSQConsumerConfig {
    topic: Arc<NSQTopic>,
    channel: Arc<NSQChannel>,
    sources: NSQConsumerConfigSources,
    shared: NSQConfigShared,
    max_in_flight: u32,
    sample_rate: Option<NSQSampleRate>,
    rebalance_interval: std::time::Duration,
    max_requeue_delay: std::time::Duration,
    base_requeue_delay: std::time::Duration,
}

impl NSQConsumerConfig {
    /// A default configuration. You will likely need to configure other
    /// options.
    pub fn new(topic: Arc<NSQTopic>, channel: Arc<NSQChannel>) -> Self {
        NSQConsumerConfig {
            sources: NSQConsumerConfigSources::Daemons(Vec::new()),
            shared: NSQConfigShared::new(),
            max_in_flight: 1,
            sample_rate: None,
            rebalance_interval: std::time::Duration::new(5, 0),
            max_requeue_delay: std::time::Duration::from_secs(60 * 15),
            base_requeue_delay: std::time::Duration::from_secs(90),
            topic,
            channel,
        }
    }

    /// The maximum number of messages to process at once shared across all
    /// connections. Defaults to a single message.
    pub fn set_max_in_flight(mut self, max_in_flight: u32) -> Self {
        self.max_in_flight = max_in_flight;

        self
    }

    /// Where an NSQ consumer should find connections. Either an explicit list
    /// of NSQ Daemons, or a list of NSQ Lookup Daemons to find NSQ
    /// instances. Defaults to no connections.
    pub fn set_sources(mut self, sources: NSQConsumerConfigSources) -> Self {
        self.sources = sources;

        self
    }

    /// NSQ Daemon connection options, such as compression and TLS.
    pub fn set_shared(mut self, shared: NSQConfigShared) -> Self {
        self.shared = shared;

        self
    }

    /// What percentage of messages to sample from the stream. Defaults to
    /// consuming all messages.
    pub fn set_sample_rate(mut self, sample_rate: NSQSampleRate) -> Self {
        self.sample_rate = Some(sample_rate);

        self
    }

    /// To maintain max in flight NSQ Daemons need to periodically have the
    /// ready count rebalanced. For example as nodes fail. Defaults to every
    /// 5 seconds.
    pub fn set_rebalance_interval(
        mut self,
        rebalance_interval: std::time::Duration,
    ) -> Self {
        self.rebalance_interval = rebalance_interval;

        self
    }

    /// The maximum limit on how long a requeued message is delayed. Defaults to
    /// 15 minutes.
    pub fn set_max_requeue_interval(
        mut self,
        interval: std::time::Duration,
    ) -> Self {
        self.max_requeue_delay = interval;

        self
    }

    /// When a message is first requeued, this controls how long it should be
    /// delayed for. Defaults to 90 seconds.
    pub fn set_base_requeue_interval(
        mut self,
        interval: std::time::Duration,
    ) -> Self {
        self.base_requeue_delay = interval;

        self
    }

    /// Construct an NSQ consumer with this configuration.
    pub fn build(self) -> NSQConsumer {
        NSQConsumer::new(self)
    }
}

struct NSQConnectionMeta {
    connection: NSQDConnection,
    found_by: HashSet<String>,
}

/// An NSQD consumer corresponding to multiple NSQD instances.
///
/// A consumer will automatically restart and maintain connections to multiple
/// NSQD instances. As connections die, and restart `REQ` will automatically be
/// rebalanced to the active connections.
pub struct NSQConsumer {
    from_connections_rx: tokio::sync::mpsc::UnboundedReceiver<NSQEvent>,
    clients_ref:
        std::sync::Arc<std::sync::RwLock<HashMap<String, NSQConnectionMeta>>>,
    oneshots: Vec<tokio::sync::oneshot::Sender<()>>,
}

#[derive(serde::Deserialize)]
struct LookupResponseProducer {
    broadcast_address: String,
    tcp_port: u16,
}

#[derive(serde::Deserialize)]
struct LookupResponse {
    producers: Vec<LookupResponseProducer>,
}

fn remove_old_connections(
    connections: &mut HashMap<String, NSQConnectionMeta>,
) {
    connections.retain(|&_, v| {
        if v.found_by.is_empty() {
            info!("dropping old connection");

            false
        } else {
            true
        }
    });
}

async fn lookup(
    address: &str,
    config: &NSQConsumerConfig,
    clients_ref: &std::sync::Arc<
        std::sync::RwLock<HashMap<String, NSQConnectionMeta>>,
    >,
    from_connections_tx: &tokio::sync::mpsc::UnboundedSender<NSQEvent>,
) -> Result<(), Error> {
    let raw_uri = (address.to_owned() + "/lookup?topic=" + &config.topic.topic)
        .to_string();

    let uri = raw_uri.parse::<hyper::Uri>()?;

    let client = hyper::Client::new();

    let response = client.get(uri).await?;

    let buffer = hyper::body::to_bytes(response).await?;

    let lookup_response: LookupResponse = serde_json::from_slice(&buffer)?;

    {
        let mut guard = clients_ref.write().unwrap();

        for producer in lookup_response.producers.iter() {
            let address = producer.broadcast_address.clone()
                + ":"
                + &producer.tcp_port.to_string();

            match guard.get_mut(&address) {
                Some(context) => {
                    context.found_by.insert(address.clone());

                    continue;
                }
                None => {
                    info!("new producer: {}", address);

                    let client = NSQDConnection::new_with_queue(
                        NSQDConfig {
                            address: address.clone(),
                            shared: config.shared.clone(),
                            sample_rate: config.sample_rate,
                            max_requeue_delay: config.max_requeue_delay,
                            base_requeue_delay: config.base_requeue_delay,
                            subscribe: Some((
                                config.topic.clone(),
                                config.channel.clone(),
                            )),
                        },
                        from_connections_tx.clone(),
                    );

                    let _ = client.queue_message(MessageToNSQ::RDY(1));

                    let mut found_by = HashSet::new();
                    found_by.insert(address.clone());

                    guard.insert(
                        address,
                        NSQConnectionMeta {
                            connection: client,
                            found_by,
                        },
                    );
                }
            }
        }

        remove_old_connections(&mut guard);
    }

    rebalancer_step(config.max_in_flight, &clients_ref).await;

    Ok(())
}

async fn lookup_supervisor(
    address: String,
    poll_interval: std::time::Duration,
    config: NSQConsumerConfig,
    clients_ref: std::sync::Arc<
        std::sync::RwLock<HashMap<String, NSQConnectionMeta>>,
    >,
    from_connections_tx: tokio::sync::mpsc::UnboundedSender<NSQEvent>,
) {
    loop {
        let f = lookup(&address, &config, &clients_ref, &from_connections_tx);

        if let Err(generic) = f.await {
            error!("lookup_supervisor unknown error {}", generic);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn rebalancer_step(
    max_in_flight: u32,
    clients_ref: &std::sync::RwLock<HashMap<String, NSQConnectionMeta>,
    >,
) -> bool {
    let guard = clients_ref.read().unwrap();

    let mut healthy = Vec::new();

    for (_, node) in guard.iter() {
        if node.connection.healthy() {
            healthy.push(&node.connection);
        }
    }

    if healthy.is_empty() {
        return false;
    }

    let partial = max_in_flight / (healthy.len() as u32);

    let partial = if partial == 0 { 1 } else { partial };

    for node in &healthy {
        let _ = NSQDConnection::queue_message(
            *node,
            MessageToNSQ::RDY(partial as u16),
        );
    }

    true
}

async fn rebalancer(
    rebalance_interval: std::time::Duration,
    max_in_flight: u32,
    clients_ref: std::sync::Arc<
        std::sync::RwLock<HashMap<String, NSQConnectionMeta>>,
    >,
) {
    loop {
        if rebalancer_step(max_in_flight, &clients_ref).await {
            tokio::time::sleep(rebalance_interval).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

impl NSQConsumer {
    fn new(config: NSQConsumerConfig) -> NSQConsumer {
        let (from_connections_tx, from_connections_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let mut pool = NSQConsumer {
            clients_ref: std::sync::Arc::new(std::sync::RwLock::new(
                HashMap::new(),
            )),
            oneshots: Vec::new(),
            from_connections_rx,
        };

        match &config.sources {
            NSQConsumerConfigSources::Daemons(daemons) => {
                let mut guard = pool.clients_ref.write().unwrap();

                for address in daemons.iter() {
                    info!("new producer: {}", address);

                    let client = NSQDConnection::new_with_queue(
                        NSQDConfig {
                            address: address.clone(),
                            shared: config.shared.clone(),
                            sample_rate: config.sample_rate,
                            max_requeue_delay: config.max_requeue_delay,
                            base_requeue_delay: config.base_requeue_delay,
                            subscribe: Some((
                                config.topic.clone(),
                                config.channel.clone(),
                            )),
                        },
                        from_connections_tx.clone(),
                    );

                    guard.insert(
                        address.clone(),
                        NSQConnectionMeta {
                            connection: client,
                            found_by: HashSet::new(),
                        },
                    );
                }
            }
            NSQConsumerConfigSources::Lookup(lookup_config) => {
                for node in lookup_config.addresses.iter() {
                    let (shutdown_tx, shutdown_rx) =
                        tokio::sync::oneshot::channel();

                    pool.oneshots.push(shutdown_tx);

                    let supervisor_future = lookup_supervisor(
                        node.clone(),
                        lookup_config.poll_interval,
                        config.clone(),
                        pool.clients_ref.clone(),
                        from_connections_tx.clone(),
                    );

                    tokio::spawn(async move {
                        with_stopper(shutdown_rx, supervisor_future).await;
                    });
                }
            }
        }

        let clients_ref_dupe = pool.clients_ref.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            with_stopper(
                shutdown_rx,
                rebalancer(
                    config.rebalance_interval,
                    config.max_in_flight,
                    clients_ref_dupe,
                ),
            )
            .await;
        });

        pool.oneshots.push(shutdown_tx);

        pool
    }

    /// Consume events from NSQ connections including status events.
    pub async fn consume(&mut self) -> Option<NSQEvent> {
        self.from_connections_rx.recv().await
    }

    /// Consume events from NSQ connections ignoring all connection status
    /// events.
    pub async fn consume_filtered(&mut self) -> Option<NSQMessage> {
        loop {
            let event = self.from_connections_rx.recv().await;

            match event {
                None => {
                    trace!("filtered {:?}", event);

                    return None;
                }
                Some(event) => match event {
                    NSQEvent::Message(message) => return Some(message),
                    _ => continue,
                },
            };
        }
    }

    /// Indicates if any connections are blocked on processing before being able to receive more
    /// messages. This can be useful for determining when to process a batch of messages.
    pub fn is_starved(&mut self) -> bool {
        let guard = self.clients_ref.read().unwrap();

        for (_, node) in guard.iter() {
            if node.connection.is_starved() {
                return true;
            }
        }

        false
    }
}

impl Drop for NSQConsumer {
    fn drop(&mut self) {
        trace!("NSQConsumer::drop()");

        while let Some(oneshot) = self.oneshots.pop() {
            oneshot.send(()).unwrap(); // other end should always exist
        }
    }
}
