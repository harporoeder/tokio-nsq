use super::*;
use connection::*;

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
        let empty = v.found_by.is_empty();

        if (empty) {
            info!("dropping old connection");
        }

        !empty
    });
}

async fn lookup(
    address:             &String,
    config:              &NSQConsumerConfig,
    clients_ref:         &std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
    from_connections_tx: &tokio::sync::mpsc::UnboundedSender<NSQEvent>
) -> Result<(), Error>
{
    let raw_uri = (address.to_owned() + "/lookup?topic=" + &config.topic).to_string();

    let uri = raw_uri.parse::<hyper::Uri>()?;

    let client = hyper::Client::new();

    let response = client.get(uri).await?;

    let buffer = hyper::body::to_bytes(response).await?;

    let s = std::str::from_utf8(&buffer)?;

    println!("got nodes: {}", s);

    let lookup_response: LookupResponse = serde_json::from_slice(&buffer)?;

    let mut guard = clients_ref.lock().unwrap();

    for producer in lookup_response.producers.iter() {
        let address = producer.broadcast_address.clone() + ":" + &producer.tcp_port.to_string();

        match guard.get_mut(&address) {
            Some(context) => {
                println!("existing producer: {}", address);

                context.found_by.insert(address.clone());

                continue;
            },
            None    => {
                println!("new producer: {}", address);

                let mut client = NSQDConnection::new_with_queue(
                    NSQDConfig {
                        address:   address.clone(),
                        subscribe: Some((config.topic.clone(), config.channel.clone())),
                        tls:       config.tls.clone(),
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
            trace!("lookup_supervisor unknown error {}", generic);
        }

        tokio::time::delay_for(std::time::Duration::new(5, 0)).await;
    }
}

#[derive(Clone)]
pub struct NSQConsumerLookupConfig {
    pub poll_interval: std::time::Duration,
    pub addresses:     HashSet<String>
}

#[derive(Clone)]
pub enum NSQConsumerConfigSources {
    Daemons(Vec<String>),
    Lookup(NSQConsumerLookupConfig)
}

#[derive(Clone)]
pub struct NSQConsumerConfig {
    pub topic:   String,
    pub channel: String,
    pub sources: NSQConsumerConfigSources,
    pub tls:     Option<NSQDConfigTLS>,
}

struct NSQConnectionMeta {
    connection: NSQDConnection,
    found_by:   HashSet<String>,
}

pub struct NSQConsumer {
    from_connections_rx: tokio::sync::mpsc::UnboundedReceiver<NSQEvent>,
    clients_ref:         std::sync::Arc<std::sync::Mutex<HashMap<String, NSQConnectionMeta>>>,
    oneshots:            Vec<tokio::sync::oneshot::Sender<()>>
}

impl NSQConsumer {
    pub fn new(config: NSQConsumerConfig) -> NSQConsumer {
        info!("NSQConsumer::new()");

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

        return pool;
    }

    pub async fn consume(&mut self) -> Option<NSQEvent> {
        match self.from_connections_rx.recv().await {
            Some(message) => Some(message),
            None          => None,
        }
    }

    pub async fn consume_filtered(&mut self) -> Option<MessageFromNSQ> {
        loop {
            let event = self.from_connections_rx.recv().await;

            match event {
                None        => {
                    println!("filtered {:?}", event);

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
        info!("NSQConsumer::drop()");

        while let Some(oneshot) = self.oneshots.pop() {
            oneshot.send(()).unwrap(); // other end should always exist
        }
    }
}
