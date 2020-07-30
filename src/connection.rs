use super::*;
use connection_config::*;

use std::sync::atomic::{AtomicU64};
use tokio_rustls::webpki::DNSNameRef;
use rustls::*;
use tokio_rustls::{ TlsConnector, rustls::ClientConfig };
use failure::Fail;
use std::fmt;
use regex::Regex;
use crate::backoff::backoff::Backoff;
use std::time::{Instant};

use compression::*;

lazy_static! {
    static ref NAMEREGEX: Regex = Regex::new(r"^[\.a-zA-Z0-9_-]+(#ephemeral)?$").unwrap();
}

fn is_valid_name(name: &String) -> bool {
    if name.len() < 1 || name.len() > 64 {
        return false;
    }

    return NAMEREGEX.is_match(name);
}

/// A smart constructor validating an NSQ topic name
#[derive(Clone, Debug)]
pub struct NSQTopic {
    pub(crate) topic: String
}

impl NSQTopic {
    /// Must match the regex `^[\.a-zA-Z0-9_-]+(#ephemeral)?$` and have length > 0 && < 65.
    pub fn new<S: Into<String>>(topic: S) -> Option<Arc<Self>> {
        let topic = topic.into();

        if is_valid_name(&topic) {
            Some(Arc::new(Self{
                topic: topic
            }))
        } else {
            None
        }
    }
}

/// A smart constructor validating an NSQ channel name
#[derive(Clone, Debug)]
pub struct NSQChannel {
    pub(crate) channel: String
}

impl NSQChannel {
    /// Must match the regex `^[\.a-zA-Z0-9_-]+(#ephemeral)?$` and have length > 0 && < 65.
    pub fn new<S: Into<String>>(channel: S) -> Option<Arc<Self>> {
        let channel = channel.into();

        if is_valid_name(&channel) {
            Some(Arc::new(Self{
                channel: channel
            }))
        } else {
            None
        }
    }
}

#[derive(Debug, Fail)]
struct NoneError;

impl fmt::Display for NoneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, Fail)]
struct ProtocolError {
    message: String
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

struct Unverified {}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _roots:           &RootCertStore,
        _presented_certs: &[Certificate],
        _dns_name:        DNSNameRef,
        _ocsp_response:   &[u8]
    ) -> Result<ServerCertVerified, TLSError>
    {
        return Ok(ServerCertVerified::assertion());
    }
}

#[derive(serde::Serialize)]
struct IdentifyBody {
    client_id:           Option<String>,
    hostname:            String,
    user_agent:          String,
    feature_negotiation: bool,
    tls_v1:              bool,
    deflate:             bool,
    sample_rate:         Option<u8>,
}

#[derive(serde::Deserialize)]
struct IdentifyResponse {
    max_rdy_count:         u32,
    version:               String,
    max_msg_timeout:       u32,
    msg_timeout:           u32,
    tls_v1:                bool,
    deflate:               bool,
    deflate_level:         u8,
    max_deflate_level:     u8,
    snappy:                bool,
    sample_rate:           u8,
    auth_required:         bool,
    output_buffer_size:    u32,
    output_buffer_timeout: u32,
}

#[derive(Debug)]
pub enum MessageToNSQ {
    NOP,
    PUB(Arc<NSQTopic>, Vec<u8>),
    DPUB(Arc<NSQTopic>, Vec<u8>, u32),
    MPUB(Arc<NSQTopic>, Vec<Vec<u8>>),
    SUB(Arc<NSQTopic>, Arc<NSQChannel>),
    RDY(u16),
    FIN([u8; 16]),
    REQ([u8; 16]),
    TOUCH([u8; 16]),
}

/// An NSQ message. If this message is dropped with being finished and the respective NSQ
/// daemon connection still exists the message will automatically by requeued.
#[derive(Debug)]
pub struct MessageFromNSQ {
        context:   Arc<NSQDConnectionShared>,
        consumed:  bool,
    pub body:      Vec<u8>,
    pub attempt:   u16,
    pub id:        [u8; 16],
    pub timestamp: u64,
}
/// An event from an NSQ connection. Includes connection status updates, errors, and actually
/// NSQ messages.
#[derive(Debug)]
pub enum NSQEvent {
    /// An NSQ consumer message
    Message(MessageFromNSQ),
    /// Generated when a connection completes a handshake
    Healthy(),
    /// Generated when a connection fails and will be restarted.
    Unhealthy(),
    /// An acknowledgement for a producer that a message was delivered.
    Ok(),
    /// A producer failed to queue a message
    SendFailed(),
}

impl MessageFromNSQ {
    /// Sends a message acknowledgement to NSQ.
    pub fn finish(mut self) {
        if self.context.healthy.load(Ordering::SeqCst) {
            let message = MessageToNSQ::FIN(self.id);

            self.context.to_connection_tx_ref.send(message).unwrap();

            self.consumed = true;
        } else {
            warn!("finish unhealthy");
        }
    }
    /// Tells NSQ daemon to reset the timeout for this message.
    pub fn touch(&mut self) {
        if self.context.healthy.load(Ordering::SeqCst) {
            let message = MessageToNSQ::TOUCH(self.id);

            self.context.to_connection_tx_ref.send(message).unwrap();
        } else {
            warn!("touch unhealthy");
        }
    }
}

impl Drop for MessageFromNSQ {
    fn drop(&mut self) {
        if !self.consumed {
            if self.context.healthy.load(Ordering::SeqCst) {
                let message = MessageToNSQ::REQ(self.id);

                self.context.to_connection_tx_ref.send(message).unwrap();
            } else {
                error!("MessageFromNSQ::drop failed");
            }
        }
    }
}

struct NSQDConnectionState {
    config:               NSQDConfig,
    from_connection_tx:   tokio::sync::mpsc::UnboundedSender<NSQEvent>,
    to_connection_rx:     tokio::sync::mpsc::UnboundedReceiver<MessageToNSQ>,
    to_connection_tx_ref: std::sync::Arc<
        tokio::sync::mpsc::UnboundedSender<MessageToNSQ>>,
    shared:               Arc<NSQDConnectionShared>,
}

#[derive(Debug)]
struct NSQDConnectionShared {
    healthy:              AtomicBool,
    to_connection_tx_ref: std::sync::Arc<tokio::sync::mpsc::UnboundedSender<MessageToNSQ>>,
    inflight:             AtomicU64,
}

struct FrameMessage {
    timestamp: u64,
    attempt:   u16,
    id:        [u8; 16],
    body:      Vec<u8>,
}

enum Frame {
    Response(Vec<u8>),
    Error(Vec<u8>),
    Message(FrameMessage),
    Unknown
}

async fn read_frame_data<S: AsyncRead + std::marker::Unpin>(
    stream: &mut S
) -> Result<Frame, Error>
{
    let mut frame_size_buffer = [0; 4];
    stream.read_exact(&mut frame_size_buffer).await?;
    let frame_size = u32::from_be_bytes(frame_size_buffer) - 4;

    let mut frame_type_buffer = [0; 4];
    stream.read_exact(&mut frame_type_buffer).await?;
    let frame_type = u32::from_be_bytes(frame_type_buffer);

    if frame_type == 0 {
        let mut frame_body = Vec::new();
        frame_body.resize(frame_size as usize, 0);
        stream.read_exact(&mut frame_body).await?;

        return Ok(Frame::Response(frame_body));
    } else if frame_type == 1 {
        let mut frame_body = Vec::new();
        frame_body.resize(frame_size as usize, 0);
        stream.read_exact(&mut frame_body).await?;

        let s = std::str::from_utf8(&frame_body)?;

        if s == "E_FIN_FAILED" || s == "E_REQ_FAILED" || s == "E_TOUCH_FAILED" {
            warn!("non fatal protocol error {}", s);

            return Ok(Frame::Error(frame_body));
        }  else {
            error!("fatal protocol error = {}", s);

            Err(ProtocolError{message: s.to_string()})?;
        }
    } else if frame_type == 2 {
        let mut message_timestamp_buffer = [0; 8];
        stream.read_exact(&mut message_timestamp_buffer).await?;
        let message_timestamp = u64::from_be_bytes(message_timestamp_buffer);

        let mut message_attempts_buffer = [0; 2];
        stream.read_exact(&mut message_attempts_buffer).await?;
        let message_attempts = u16::from_be_bytes(message_attempts_buffer);

        let mut message_id = [0; 16];
        stream.read_exact(&mut message_id).await?;

        let body_size = frame_size - 8 - 2 - 16;
        let mut message_body = Vec::new();
        message_body.resize(body_size as usize, 0);
        stream.read_exact(&mut message_body).await?;

        return Ok(Frame::Message(FrameMessage {
            timestamp: message_timestamp,
            attempt:   message_attempts,
            id:        message_id,
            body:      message_body,
        }));
    } else {
        error!("frame_type unknown = {}", frame_type);
    }

    return Ok(Frame::Unknown);
}

async fn handle_reads<S: AsyncRead + std::marker::Unpin>(
    stream:               &mut S,
    shared:               &Arc<NSQDConnectionShared>,
    from_connection_tx:   &mut tokio::sync::mpsc::UnboundedSender<NSQEvent>
) -> Result<(), Error>
{
    loop {
        match read_frame_data(stream).await? {
            Frame::Response(body) => {
                let frame_body_str = std::str::from_utf8(&body)?;

                if frame_body_str == "_heartbeat_" {
                    shared.to_connection_tx_ref.send(MessageToNSQ::NOP)?;
                } else if frame_body_str == "OK" {
                    from_connection_tx.send(NSQEvent::Ok())?;
                }

                continue;
            }
            Frame::Error(_) => {
                continue;
            }
            Frame::Message(message) => {
                from_connection_tx.send(NSQEvent::Message(MessageFromNSQ{
                    context:   shared.clone(),
                    consumed:  false,
                    body:      message.body,
                    attempt:   message.attempt,
                    id:        message.id,
                    timestamp: message.timestamp,
                }))?;

                shared.inflight.fetch_add(1, Ordering::SeqCst);

                continue;
            }
            Frame::Unknown => {
                continue;
            }
        }
    }
}

async fn write_fin<S: AsyncWrite + std::marker::Unpin>(
    stream: &mut S,
    id:     &[u8]
) -> Result<(), Error>
{
    stream.write_all(b"FIN ").await?;
    stream.write_all(&id).await?;
    stream.write_all(b"\n").await?;
    return Ok(());
}

async fn write_rdy<S: AsyncWrite + std::marker::Unpin>(
    stream: &mut S,
    count:  u16,
) -> Result<(), Error>
{
    stream.write_all(b"RDY ").await?;
    stream.write_all(count.to_string().as_bytes()).await?;
    stream.write_all(b"\n").await?;
    return Ok(());
}

async fn write_touch<S: AsyncWrite + std::marker::Unpin>(
    stream: &mut S,
    id:     &[u8]
) -> Result<(), Error>
{
    stream.write_all(b"TOUCH ").await?;
    stream.write_all(&id).await?;
    stream.write_all(b"\n").await?;
    return Ok(());
}

async fn write_requeue<S: AsyncWrite + std::marker::Unpin>(
    stream: &mut S,
    id:     &[u8]
) -> Result<(), Error>
{
    stream.write_all(b"REQ ").await?;
    stream.write_all(&id).await?;
    stream.write_all(b" 0\n").await?;
    return Ok(());
}

async fn write_auth<S: AsyncWrite + std::marker::Unpin>(
    stream:      &mut S,
    credentials: &[u8]
) -> Result<(), Error>
{
    stream.write_all(b"AUTH\n").await?;
    let count = u32::try_from(credentials.len())?.to_be_bytes();
    stream.write_all(&count).await?;
    stream.write_all(&credentials).await?;
    return Ok(());
}

async fn handle_single_command<S: AsyncWrite + std::marker::Unpin>(
    _shared:  &Arc<NSQDConnectionShared>,
    message:  MessageToNSQ,
    stream:   &mut S
) -> Result<(), Error>
{
    match message {
        MessageToNSQ::NOP => {
            stream.write_all(b"NOP\n").await?;
        },
        MessageToNSQ::PUB(topic, body) => {
            stream.write_all(b"PUB ").await?;
            stream.write_all(topic.topic.as_bytes()).await?;
            stream.write_all(b"\n").await?;

            let count = u32::try_from(body.len())?.to_be_bytes();

            stream.write_all(&count).await?;
            stream.write_all(&body).await?;
        },
        MessageToNSQ::DPUB(topic, body, delay) => {
            stream.write_all(b"DPUB ").await?;
            stream.write_all(topic.topic.as_bytes()).await?;
            stream.write_all(b" ").await?;
            stream.write_all(delay.to_string().as_bytes()).await?;
            stream.write_all(b"\n").await?;

            let count = u32::try_from(body.len())?.to_be_bytes();

            stream.write_all(&count).await?;
            stream.write_all(&body).await?;
        },
        MessageToNSQ::MPUB(topic, messages) => {
            let body_bytes = messages.iter().fold(0, |sum, x|
                4 + sum + x.len()
            ) + 4;

            stream.write_all(b"MPUB ").await?;
            stream.write_all(topic.topic.as_bytes()).await?;
            stream.write_all(b"\n").await?;

            let body_bytes = u32::try_from(body_bytes)?.to_be_bytes();
            stream.write_all(&body_bytes).await?;

            let count = u32::try_from(messages.len())?.to_be_bytes();
            stream.write_all(&count).await?;

            for message in messages.iter() {
                let message_size = u32::try_from(message.len())?.to_be_bytes();
                stream.write_all(&message_size).await?;
                stream.write_all(&message).await?;
            }
        },
        MessageToNSQ::SUB(topic, channel) => {
            stream.write_all(b"SUB ").await?;
            stream.write_all(topic.topic.as_bytes()).await?;
            stream.write_all(b" ").await?;
            stream.write_all(channel.channel.as_bytes()).await?;
            stream.write_all(b"\n").await?;
        },
        MessageToNSQ::RDY(count) => {
            write_rdy(stream, count).await?;
        },
        MessageToNSQ::FIN(id) => {
            write_fin(stream, &id).await?;
        },
        MessageToNSQ::TOUCH(id) => {
            write_touch(stream, &id).await?;
        },
        MessageToNSQ::REQ(id) => {
            write_requeue(stream, &id).await?;
        },
    }

    return Ok(());
}

async fn handle_command<S: AsyncWrite + std::marker::Unpin>(
    shared:           &Arc<NSQDConnectionShared>,
    to_connection_rx: &mut tokio::sync::mpsc::UnboundedReceiver<MessageToNSQ>,
    stream:           &mut S
) -> Result<(), Error>
{
    loop {
        let message = to_connection_rx.recv().await.ok_or(NoneError)?;

        handle_single_command(shared, message, stream).await?;
    }
}

async fn handle_stop(stop: &mut tokio::sync::oneshot::Receiver<()>) {
    let _ = stop.await;
}

async fn run_generic<W: AsyncWrite + std::marker::Unpin, R: AsyncRead + std::marker::Unpin>(
    state:     &mut NSQDConnectionState,
    mut stream_rx: R,
    mut stream_tx: W,
) -> Result<(), Error>
{
    match &state.config.subscribe {
        Some((channel, topic)) => {
            handle_single_command(&state.shared, MessageToNSQ::SUB(channel.clone(), topic.clone()),
                &mut stream_tx).await?;

            match read_frame_data(&mut stream_rx).await? {
                Frame::Response(_) => {

                },
                _ => {
                    return Ok(());
                }
            }
        }
        None => {}
    }

    &state.shared.healthy.store(true, Ordering::SeqCst);

    state.from_connection_tx.send(NSQEvent::Healthy())?;

    let f1 = handle_command(&state.shared, &mut state.to_connection_rx, &mut stream_tx);
    let f2 = handle_reads(&mut stream_rx, &state.shared, &mut state.from_connection_tx);

    tokio::select! {
        status = f1 => {
            status?;
        }
        status = f2 => {
            status?;
        }
    };

    return Ok(());
}

fn write_to_dyn<S: Send + AsyncWrite + std::marker::Unpin + 'static>(stream_tx: S)
    -> Box<dyn Send + AsyncWrite + std::marker::Unpin>
{
    Box::new(stream_tx)
}

fn read_to_dyn<S: Send + AsyncRead + std::marker::Unpin + 'static>(stream_rx: S)
    -> Box<dyn Send + AsyncRead + std::marker::Unpin>
{
    Box::new(stream_rx)
}

async fn run_connection(state: &mut NSQDConnectionState) -> Result<(), Error> {
    let mut stream = tokio::net::TcpStream::connect(state.config.address.clone()).await?;

    let identify_body = IdentifyBody {
        client_id:           state.config.shared.client_id.clone(),
        hostname:            "my-hostname".to_string(),
        user_agent:          "rustnsq/0.1.0".to_string(),
        feature_negotiation: true,
        tls_v1:              state.config.shared.tls.is_some(),
        deflate:             state.config.shared.compression.is_some(),
        sample_rate:         None,
    };

    let serialized = serde_json::to_string(&identify_body)?;

    let count = u32::try_from(serialized.len())?.to_be_bytes();

    stream.write_all(b"  V2").await?;
    stream.write_all(b"IDENTIFY\n").await?;
    stream.write_all(&count).await?;
    stream.write_all(serialized.as_bytes()).await?;

    let settings: IdentifyResponse = match read_frame_data(&mut stream).await? {
        Frame::Response(body) => {
            serde_json::from_slice(&body)?
        }
        _ => {
            return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                 "feature negotiation failed")));
        }
    };

    let config_tls =
        if let Some(config_tls) = &state.config.shared.tls {
            if config_tls.required && !settings.tls_v1 {
                return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                     "tls required but not supported by nsqd")));
            }

            if !settings.tls_v1 {
                None
            } else {
                Some(config_tls)
            }
        } else {
            None
        };

    let (stream_rx, stream_tx) = if let Some(config_tls) = config_tls {
        let verifier = Arc::new(Unverified{});

        let config = match &config_tls.client_config {
            Some(client_config) => {
                client_config.clone()
            },
            None => {
                let mut config = ClientConfig::new();
                config.dangerous().set_certificate_verifier(verifier);
                Arc::new(config)
            }
        };

        let config = TlsConnector::from(config);
        let dnsname = DNSNameRef::try_from_ascii_str("abctest.com")?;

        let stream = config.connect(dnsname, stream).await?;

        let (mut stream_rx, stream_tx) = tokio::io::split(stream);

        match read_frame_data(&mut stream_rx).await? {
            Frame::Response(body) => {
                if body != "OK".as_bytes() {
                    return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                        "tls negotiation expected OK")));
                }
            }
            _ => {
                return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                     "tls negotiation failed")));
            }
        }

        (read_to_dyn(stream_rx), write_to_dyn(stream_tx))
    } else {
        let (stream_rx, stream_tx) = tokio::io::split(stream);

        (read_to_dyn(stream_rx), write_to_dyn(stream_tx))
    };

    let (mut stream_rx, mut stream_tx) = if state.config.shared.compression.is_some() {
        let mut stream_rx = NSQInflate::new(stream_rx);
        let stream_tx     = NSQDeflate::new(stream_tx);

        match read_frame_data(&mut stream_rx).await? {
            Frame::Response(body) => {
                if body != "OK".as_bytes() {
                    return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                        "compression negotiation expected OK")));
                }
            }
            _ => {
                return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                     "compression negotiation failed")));
            }
        }

        (read_to_dyn(stream_rx), write_to_dyn(stream_tx))
    } else {
        (stream_rx, stream_tx)
    };

    if let Some(credentials) = &state.config.shared.credentials {
        write_auth(&mut stream_tx, credentials).await?;

        match read_frame_data(&mut stream_rx).await? {
            Frame::Response(_body) => {

            }
            _ => {
                return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                     "authentication failed")));
            }
        }
    }

    info!("handshake completed");

    run_generic(state, stream_rx, stream_tx).await?;

    return Ok(());
}

pub async fn with_stopper(
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    operation:   impl std::future::Future
) {
    tokio::select! {
        _ = shutdown_rx => {
            info!("stopper operation stopped via oneshot");
        },
        _ = operation => {
            info!("stopper operation finished naturally");
        }
    }
}

async fn run_connection_supervisor(mut state: NSQDConnectionState) {
    let mut backoff = backoff::ExponentialBackoff::default();
    backoff.max_interval = std::time::Duration::new(60, 0);

    loop {
        let now = Instant::now();

        match run_connection(&mut state).await {
            Err(generic) => {
                &state.shared.healthy.store(false, Ordering::SeqCst);

                let _ = state.from_connection_tx.send(NSQEvent::Unhealthy());

                if let Some(error) = generic.downcast_ref::<tokio::io::Error>() {
                    error!("tokio io error: {}", error);
                } else if let Some(error) = generic.downcast_ref::<serde_json::Error>() {
                    error!("serde json error: {}", error);

                    return;
                } else {
                    error!("unknown error {}", generic);

                    return;
                }
            },
            _ => {
                return;
            }
        }

        let mut drained: u64 = 0;

        loop {
            match state.to_connection_rx.try_recv() {
                Ok(_) => {
                    drained = drained + 1;
                },
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                },
                Err(tokio::sync::mpsc::error::TryRecvError::Closed) => {
                    return;
                }
            }
        }

        if drained != 0 {
            warn!("drained {} messages", drained);
        }

        if now.elapsed().as_secs() >= 45 {
            info!("run_connection_supervisor resetting backoff");

            backoff.reset();
        }

        let sleep_for = backoff.next_backoff().unwrap();
        info!("run_connection_supervisor sleeping for: {}", sleep_for.as_secs());
        tokio::time::delay_for(sleep_for).await;
    }
}

#[derive(Clone)]
pub struct NSQDConfig {
    pub address:     String,
    pub subscribe:   Option<(Arc<NSQTopic>, Arc<NSQChannel>)>,
    pub shared:      NSQConfigShared,
    pub sample_rate: Option<u8>,
}

pub struct NSQDConnection {
    shutdown_tx:          Option<tokio::sync::oneshot::Sender<()>>,
    from_connection_rx:   tokio::sync::mpsc::UnboundedReceiver<NSQEvent>,
    to_connection_tx_ref: std::sync::Arc<
        tokio::sync::mpsc::UnboundedSender<MessageToNSQ>>,
    shared:               Arc<NSQDConnectionShared>,
}

impl NSQDConnection {
    pub fn new(config: NSQDConfig) -> NSQDConnection {
        let (from_connection_tx, from_connection_rx) = tokio::sync::mpsc::unbounded_channel();

        return NSQDConnection::new_with_queues(config, from_connection_tx, from_connection_rx);
    }

    pub fn new_with_queue(
        config:             NSQDConfig,
        from_connection_tx: tokio::sync::mpsc::UnboundedSender<NSQEvent>
    ) -> NSQDConnection {
        let (_, from_connection_rx) = tokio::sync::mpsc::unbounded_channel();

        return NSQDConnection::new_with_queues(config, from_connection_tx, from_connection_rx);
    }

    fn new_with_queues(
        config:             NSQDConfig,
        from_connection_tx: tokio::sync::mpsc::UnboundedSender<NSQEvent>,
        from_connection_rx: tokio::sync::mpsc::UnboundedReceiver<NSQEvent>
    ) -> NSQDConnection {
        let (write_shutdown, read_shutdown)      = tokio::sync::oneshot::channel();
        let (to_connection_tx, to_connection_rx) = tokio::sync::mpsc::unbounded_channel();

        let to_connection_tx_ref_1 = std::sync::Arc::new(to_connection_tx);
        let to_connection_tx_ref_2 = to_connection_tx_ref_1.clone();

        let shared_state = Arc::new(NSQDConnectionShared{
            healthy:              AtomicBool::new(false),
            to_connection_tx_ref: to_connection_tx_ref_1.clone(),
            inflight:             AtomicU64::new(0),
        });

        let shared_state_clone = shared_state.clone();

        tokio::spawn(async move {
            with_stopper(read_shutdown,
                run_connection_supervisor(NSQDConnectionState {
                    config:               config,
                    from_connection_tx:   from_connection_tx,
                    to_connection_rx:     to_connection_rx,
                    to_connection_tx_ref: to_connection_tx_ref_1,
                    shared:               shared_state_clone,
                }
            )).await;
        });

        return NSQDConnection {
            shutdown_tx:          Some(write_shutdown),
            from_connection_rx:   from_connection_rx,
            to_connection_tx_ref: to_connection_tx_ref_2,
            shared:               shared_state,
        };
    }

    pub fn healthy(&self) -> bool {
        return self.shared.healthy.load(Ordering::SeqCst);
    }

    pub async fn consume(&mut self) -> Option<NSQEvent> {
        self.from_connection_rx.recv().await
    }

    fn queue_message(&mut self, message: MessageToNSQ) -> Result<(), Error> {
        if self.shared.healthy.load(Ordering::SeqCst) {
            if let Err(_) = self.to_connection_tx_ref.send(message) {
                return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                     "queue message lock failed")));
            }

            return Ok(());
        } else {
            warn!("queue message unhealthy");

            return Err(Error::from(std::io::Error::new(std::io::ErrorKind::Other,
                 "connection is disconnected")));
        }
    }

    pub fn publish(&mut self, topic: Arc<NSQTopic>, value: Vec<u8>) -> Result<(), Error> {
        self.queue_message(MessageToNSQ::PUB(topic, value))
    }

    pub fn publish_deferred(&mut self, topic: Arc<NSQTopic>, value: Vec<u8>, delay_seconds: u32)
        -> Result<(), Error>
    {
        self.queue_message(MessageToNSQ::DPUB(topic, value, delay_seconds))
    }

    pub fn publish_multiple(&mut self, topic: Arc<NSQTopic>, value: Vec<Vec<u8>>)
        -> Result<(), Error>
    {
        self.queue_message(MessageToNSQ::MPUB(topic, value))
    }

    pub fn ready(&mut self, count: u16) -> Result<(), Error> {
        self.queue_message(MessageToNSQ::RDY(count))
    }
}

impl Drop for NSQDConnection {
    fn drop(&mut self) {
        trace!("NSQDConnection::drop()");
        self.shutdown_tx.take().unwrap().send(()).unwrap() // other end should always exist
    }
}
