use super::*;
use connection::*;
use connection_config::*;

/// Configuration object for NSQLookup nodes
#[derive(Clone)]
pub struct NSQConsumerLookupConfig {
    poll_interval: std::time::Duration,
    addresses:     HashSet<String>
}

impl NSQConsumerLookupConfig {
    pub fn new() -> Self {
        NSQConsumerLookupConfig {
            poll_interval: std::time::Duration::new(60, 0),
            addresses:     HashSet::new(),
        }
    }

    pub fn set_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;

        return self;
    }

    pub fn set_addresses(mut self, addresses: HashSet<String>) -> Self {
        self.addresses = addresses;

        return self;
    }
}

/// The strategy a consumer should use to connect to NSQD instances
#[derive(Clone)]
pub enum NSQConsumerConfigSources {
    Daemons(Vec<String>),
    Lookup(NSQConsumerLookupConfig)
}

/// Configuration object for an NSQD consumer
#[derive(Clone)]
pub struct NSQConsumerConfig {
    topic:         Arc<NSQTopic>,
    channel:       Arc<NSQChannel>,
    sources:       NSQConsumerConfigSources,
    shared:        NSQConfigShared,
    max_in_flight: u32,
    sample_rate:   Option<u8>,
}

impl NSQConsumerConfig {
    pub fn new(topic: Arc<NSQTopic>, channel: Arc<NSQChannel>) -> Self {
        return NSQConsumerConfig {
            topic:         topic,
            channel:       channel,
            sources:       NSQConsumerConfigSources::Daemons(Vec::new()),
            shared:        NSQConfigShared::new(),
            max_in_flight: 1,
            sample_rate:   None,
        }
    }

    pub fn set_max_in_flight(mut self, max_in_flight: u32) -> Self {
        self.max_in_flight = max_in_flight;

        return self;
    }

    pub fn set_sources(mut self, sources: NSQConsumerConfigSources) -> Self {
        self.sources = sources;

        return self;
    }

    pub fn set_shared(mut self, shared: NSQConfigShared) -> Self {
        self.shared = shared;

        return self;
    }

    pub fn set_sample_rate(mut self, sample_rate: u8) -> Self {
        self.sample_rate = Some(sample_rate);

        return self;
    }

    pub fn build(self) -> NSQConsumer {
        return NSQConsumer::new(self);
    }
}

struct NSQConnectionMeta {
    connection: NSQDConnection,
    found_by:   HashSet<String>,
}

/// An NSQD consumer corresponding to multiple NSQD instances
pub struct NSQConsumer {
    from_connections_rx: tokio::sync::mpsc::UnboundedReceiver<NSQEvent>,
    clients_ref:         std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
    oneshots:            Vec<tokio::sync::oneshot::Sender<()>>
}

#[derive(serde::Deserialize)]
struct LookupResponseProducer {
    broadcast_address: String,
    tcp_port:          u16,
}

#[derive(serde::Deserialize)]
struct LookupResponse {
    producers: Vec<LookupResponseProducer>,
}

fn remove_old_connections(connections: &mut HashMap<String, NSQConnectionMeta>) {
    connections.retain(|&_, v| {
        if v.found_by.is_empty() {
            info!("dropping old connection");

            return false;
        } else {
            return true
        }
    });
}

async fn lookup(
    address:             &String,
    config:              &NSQConsumerConfig,
    clients_ref:         &std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
    from_connections_tx: &tokio::sync::mpsc::UnboundedSender<NSQEvent>
) -> Result<(), Error>
{
    let raw_uri = (address.to_owned() + "/lookup?topic=" + &config.topic.topic).to_string();

    let uri = raw_uri.parse::<hyper::Uri>()?;

    let client = hyper::Client::new();

    let response = client.get(uri).await?;

    let buffer = hyper::body::to_bytes(response).await?;

    let lookup_response: LookupResponse = serde_json::from_slice(&buffer)?;

    {
        let mut guard = clients_ref.lock().unwrap();

        for producer in lookup_response.producers.iter() {
            let address =
                producer.broadcast_address.clone() + ":" + &producer.tcp_port.to_string();

            match guard.get_mut(&address) {
                Some(context) => {
                    context.found_by.insert(address.clone());

                    continue;
                },
                None    => {
                    info!("new producer: {}", address);

                    let mut client = NSQDConnection::new_with_queue(
                        NSQDConfig {
                            address:     address.clone(),
                            subscribe:   Some((config.topic.clone(), config.channel.clone())),
                            shared:      config.shared.clone(),
                            sample_rate: config.sample_rate,
                        },
                        from_connections_tx.clone()
                    );

                    client.ready(1)?;

                    let mut found_by = HashSet::new();
                    found_by.insert(address.clone());

                    guard.insert(address, NSQConnectionMeta{
                        connection: client,
                        found_by:   found_by,
                    });
                }
            }
        }

        remove_old_connections(&mut guard);
    }

    rebalancer_step(config.max_in_flight, &clients_ref).await;

    return Ok(());
}

async fn lookup_supervisor(
    address:             String,
    config:              NSQConsumerConfig,
    clients_ref:         std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
    from_connections_tx: tokio::sync::mpsc::UnboundedSender<NSQEvent>
) {
    loop {
        let f = lookup(&address, &config, &clients_ref, &from_connections_tx);

        if let Err(generic) = f.await {
            error!("lookup_supervisor unknown error {}", generic);
        }

        tokio::time::delay_for(std::time::Duration::new(5, 0)).await;
    }
}

async fn rebalancer_step(
    max_in_flight: u32,
    clients_ref:   &std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
) {
    let mut guard = clients_ref.lock().unwrap();

    let mut healthy = Vec::new();

    for (_, node) in guard.iter_mut() {
        if node.connection.healthy() {
            healthy.push(&mut node.connection);
        }
    }

    if healthy.len() == 0 {
        return;
    }

    let partial = max_in_flight / (healthy.len() as u32);

    let partial = if partial == 0 {
        1
    } else {
        partial
    };

    for node in healthy.iter_mut() {
        NSQDConnection::ready(*node, partial as u16).unwrap();
    }
}

async fn rebalancer(
    max_in_flight: u32,
    clients_ref:   std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
) {
    loop {
        rebalancer_step(max_in_flight, &clients_ref).await;

        tokio::time::delay_for(std::time::Duration::new(30, 0)).await;
    }
}

impl NSQConsumer {
    fn new(config: NSQConsumerConfig) -> NSQConsumer {
        let (from_connections_tx, from_connections_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut pool = NSQConsumer {
            from_connections_rx: from_connections_rx,
            clients_ref:         std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            oneshots:            Vec::new(),
        };

        match &config.sources {
            NSQConsumerConfigSources::Daemons(_) => {

            },
            NSQConsumerConfigSources::Lookup(nodes) => {
                for node in nodes.addresses.iter() {
                    let clients_ref_dupe           = pool.clients_ref.clone();
                    let from_connections_tx_dupe   = from_connections_tx.clone();
                    let config_dupe                = config.clone();
                    let address_dupe               = node.clone();
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

                    pool.oneshots.push(shutdown_tx);

                    tokio::spawn(async move {
                        with_stopper(shutdown_rx,
                            lookup_supervisor(
                                address_dupe,
                                config_dupe,
                                clients_ref_dupe,
                                from_connections_tx_dupe
                            )
                        ).await;
                    });
                }
            }
        }

        let clients_ref_dupe           = pool.clients_ref.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            with_stopper(shutdown_rx, rebalancer(config.max_in_flight, clients_ref_dupe)).await;
        });

        pool.oneshots.push(shutdown_tx);

        return pool;
    }

    pub async fn consume(&mut self) -> Option<NSQEvent> {
        self.from_connections_rx.recv().await
    }

    pub async fn consume_filtered(&mut self) -> Option<MessageFromNSQ> {
        loop {
            let event = self.from_connections_rx.recv().await;

            match event {
                None        => {
                    trace!("filtered {:?}", event);

                    return None;
                },
                Some(event) => match event {
                    NSQEvent::Message(message) => return Some(message),
                    _                          => continue,
                }
            };
        }
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
