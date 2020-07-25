use super::*;

use std::sync::atomic::{AtomicU64};
use tokio_rustls::webpki::DNSNameRef;
use rustls::*;
use tokio_rustls::{ TlsConnector, rustls::ClientConfig };
use failure::Fail;
use std::fmt;


#[derive(Debug, Fail)]
struct NoneError;

impl fmt::Display for NoneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

struct Unverified {}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _roots: &RootCertStore,
        _presented_certs: &[Certificate],
        _dns_name: DNSNameRef,
        _ocsp_response: &[u8]
    ) -> Result<ServerCertVerified, TLSError>
    {
        info!("called verifier");

        return Ok(ServerCertVerified::assertion());
    }
}

#[derive(serde::Serialize)]
struct IdentifyBody {
    client_id:           String,
    hostname:            String,
    user_agent:          String,
    feature_negotiation: bool,
    tls_v1:              bool,
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
    PUB(String, Vec<u8>),
    SUB(String, String),
    RDY(u16),
    FIN([u8; 16]),
    REQ([u8; 16]),
    TOUCH([u8; 16]),
}

#[derive(Debug)]
pub struct MessageFromNSQ {
        context:   Arc<NSQDConnectionShared>,
        consumed:  bool,
    pub body:      Vec<u8>,
    pub attempt:   u16,
    pub id:        [u8; 16],
    pub timestamp: u64,
}

#[derive(Debug)]
pub enum NSQEvent {
    Message(MessageFromNSQ),
    Healthy(),
    Unhealthy(),
    Ok(),
    SendFailed(),
}

impl MessageFromNSQ {
    pub fn finish(mut self) {
        if self.context.healthy.load(Ordering::SeqCst) {
            trace!("finish healthy");

            let message = MessageToNSQ::FIN(self.id);

            self.context.to_connection_tx_ref.send(message).unwrap();

            self.consumed = true;
        } else {
            trace!("finish unhealthy");
        }
    }

    pub fn touch(&mut self) {
        if self.context.healthy.load(Ordering::SeqCst) {
            trace!("touch healthy");

            let message = MessageToNSQ::TOUCH(self.id);

            self.context.to_connection_tx_ref.send(message).unwrap();
        } else {
            trace!("touch unhealthy");
        }
    }
}

impl Drop for MessageFromNSQ {
    fn drop(&mut self) {
        if !self.consumed {
            if self.context.healthy.load(Ordering::SeqCst) {
                trace!("MessageFromNSQ::drop requeue success");

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
    trace!("read_frame()");

    let mut frame_size_buffer = [0; 4];
    stream.read_exact(&mut frame_size_buffer).await?;
    let frame_size = u32::from_be_bytes(frame_size_buffer) - 4;
    trace!("frame_size = {}", frame_size);

    let mut frame_type_buffer = [0; 4];
    stream.read_exact(&mut frame_type_buffer).await?;
    let frame_type = u32::from_be_bytes(frame_type_buffer);

    if frame_type == 0 {
        trace!("frame_type FrameTypeResponse");

        let mut frame_body = Vec::new();
        frame_body.resize(frame_size as usize, 0);
        stream.read_exact(&mut frame_body).await?;

        let frame_body_str = std::str::from_utf8(&frame_body)?;

        trace!("body = {}", frame_body_str);

        return Ok(Frame::Response(frame_body));
    } else if frame_type == 1 {
        trace!("frame_type FrameTypeError");

        let mut frame_body = Vec::new();
        frame_body.resize(frame_size as usize, 0);
        stream.read_exact(&mut frame_body).await?;

        let frame_body_str = std::str::from_utf8(&frame_body)?;

        trace!("body = {}", frame_body_str);

        return Ok(Frame::Error(frame_body));
    } else if frame_type == 2 {
        trace!("frame_type FrameTypeMessage");

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
        trace!("frame_type unknown = {}", frame_type);
    }

    return Ok(Frame::Unknown);
}

async fn handle_reads<S: AsyncRead + std::marker::Unpin>(
    stream:               &mut S,
    shared:               &Arc<NSQDConnectionShared>,
    from_connection_tx:   &mut tokio::sync::mpsc::UnboundedSender<NSQEvent>
) -> Result<(), Error>
{
    error!("handle_reads_loop");

    loop {
        match read_frame_data(stream).await? {
            Frame::Response(body) => {
                let frame_body_str = std::str::from_utf8(&body)?;

                if frame_body_str == "_heartbeat_" {
                    trace!("heartbeat");

                    shared.to_connection_tx_ref.send(MessageToNSQ::NOP)?;
                } else if frame_body_str == "OK" {
                    trace!("OK");

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

async fn handle_single_command<S: AsyncWrite + std::marker::Unpin>(
    shared:  &Arc<NSQDConnectionShared>,
    message: MessageToNSQ,
    stream:  &mut S
) -> Result<(), Error>
{
    match message {
        MessageToNSQ::NOP => {
            trace!("MessageToNSQ::NOP");

            stream.write_all(b"NOP\n").await?;
        },
        MessageToNSQ::PUB(channel, body) => {
            trace!("MessageToNSQ::PUB start");

            stream.write_all(b"PUB ").await?;
            stream.write_all(channel.as_bytes()).await?;
            stream.write_all(b"\n").await?;

            let count = u32::try_from(body.len())?.to_be_bytes();

            stream.write_all(&count).await?;
            stream.write_all(&body).await?;

            trace!("MessageToNSQ::PUB finished");
        },
        MessageToNSQ::SUB(topic, channel) => {
            trace!("MessageToNSQ::SUB");

            stream.write_all(b"SUB ").await?;
            stream.write_all(topic.as_bytes()).await?;
            stream.write_all(b" ").await?;
            stream.write_all(channel.as_bytes()).await?;
            stream.write_all(b"\n").await?;
        },
        MessageToNSQ::RDY(count) => {
            trace!("MessageToNSQ::RDY");

            write_rdy(stream, count).await?;
        },
        MessageToNSQ::FIN(id) => {
            trace!("MessageToNSQ::FIN");

            write_fin(stream, &id).await?;

            let inflight = shared.inflight.fetch_sub(1, Ordering::SeqCst);

            trace!("MessageToNSQ::FIN had {} inflight", inflight);
        },
        MessageToNSQ::TOUCH(id) => {
            trace!("MessageToNSQ::TOUCH");

            write_touch(stream, &id).await?;
        },
        MessageToNSQ::REQ(_id) => {
            trace!("MessageToNSQ::REQ");
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
    error!("handle_commands_loop");

    loop {
        let message = to_connection_rx.recv().await.ok_or(NoneError)?;
        trace!("to_connection_rx got message");

        handle_single_command(shared, message, stream).await?;
    }
}

async fn handle_stop(stop: &mut tokio::sync::oneshot::Receiver<()>) {
    let _ = stop.await;
}

async fn run_generic<S: AsyncWrite + AsyncRead + std::marker::Unpin>(
    state:  &mut NSQDConnectionState,
    stream: S,
) -> Result<(), Error>
{
    let (mut stream_rx, mut stream_tx) = tokio::io::split(stream);

    match &state.config.subscribe {
        Some((channel, topic)) => {
            handle_single_command(&state.shared, MessageToNSQ::SUB(channel.clone(), topic.clone()),
                &mut stream_tx).await?;

            match read_frame_data(&mut stream_rx).await? {
                Frame::Response(_) => {
                    // expected
                    trace!("subscibe success");
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


    trace!("starting main loop");


    let f1 = handle_command(&state.shared, &mut state.to_connection_rx, &mut stream_tx);
    let f2 = handle_reads(&mut stream_rx, &state.shared, &mut state.from_connection_tx);

    tokio::select! {
        status = f1 => {
            trace!("select! finished handle command");

            status?;
        }
        status = f2 => {
            trace!("select! finished handle frame");

            status?;
        }
    };

    error!("unexpected end");

    return Ok(());
}

async fn run_connection(state: &mut NSQDConnectionState) -> Result<(), Error> {
    let mut stream = tokio::net::TcpStream::connect(state.config.address.clone()).await?;

    let identify_body = IdentifyBody {
        client_id:           "my-client-id".to_string(),
        hostname:            "my-hostname".to_string(),
        user_agent:          "rustnsq/0.1.0".to_string(),
        feature_negotiation: true,
        tls_v1:              state.config.tls.is_some(),
    };

    let serialized = serde_json::to_string(&identify_body)?;
    trace!("serialized = {}", serialized);

    let count = u32::try_from(serialized.len())?.to_be_bytes();

    stream.write_all(b"  V2").await?;
    stream.write_all(b"IDENTIFY\n").await?;
    stream.write_all(&count).await?;
    stream.write_all(serialized.as_bytes()).await?;

    match read_frame_data(&mut stream).await? {
        Frame::Response(body) => {
            let _settings: IdentifyResponse = serde_json::from_slice(&body)?;
        }
        _ => {
            error!("expected response 1");

            return Ok(());
        }
    }

    if let Some(_) = &state.config.tls {
        let verifier = Arc::new(Unverified{});

        let mut config = ClientConfig::new();
        config.dangerous().set_certificate_verifier(verifier);

        let config = TlsConnector::from(Arc::new(config));
        let dnsname = DNSNameRef::try_from_ascii_str("abctest.com")?;

        let mut stream = config.connect(dnsname, stream).await?;

        match read_frame_data(&mut stream).await? {
            Frame::Response(_body) => {
                error!("got response 1");
            }
            _ => {
                error!("expected response 2");

                return Ok(());
            }
        }

        run_generic(state, stream).await?;
    } else {
        run_generic(state, stream).await?;
    };

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
    loop {
        match run_connection(&mut state).await {
            Err(generic) => {
                &state.shared.healthy.store(false, Ordering::SeqCst);

                let _ = state.from_connection_tx.send(NSQEvent::Unhealthy());

                if let Some(error) = generic.downcast_ref::<tokio::io::Error>() {
                    trace!("tokio io error: {}", error);

                    tokio::time::delay_for(std::time::Duration::new(5, 0)).await;
                } else if let Some(error) = generic.downcast_ref::<serde_json::Error>() {
                    trace!("serde json error: {}", error);

                    return;
                } else {
                    trace!("unknown error {}", generic);

                    return;
                }
            },
            _ => {
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NSQDConfigTLS {

}

#[derive(Debug, Clone)]
pub struct NSQDConfig {
    pub address:   String,
    pub subscribe: Option<(String, String)>,
    pub tls:       Option<NSQDConfigTLS>,
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

    pub fn publish(&mut self, topic: String, value: Vec<u8>) {
        if self.shared.healthy.load(Ordering::SeqCst) {
            trace!("publish healthy");

            let message = MessageToNSQ::PUB(topic, value);

            self.to_connection_tx_ref.send(message).unwrap();
        } else {
            trace!("publish unhealthy");
        }
    }

    pub fn ready(&mut self, count: u16) -> Result<(), Error> {
        trace!("ready()");

        let message = MessageToNSQ::RDY(count);

        self.to_connection_tx_ref.send(message).unwrap();

        return Ok(());
    }
}

impl Drop for NSQDConnection {
    fn drop(&mut self) {
        info!("NSQDConnection::drop()");
        self.shutdown_tx.take().unwrap().send(()).unwrap() // other end should always exist
    }
}
