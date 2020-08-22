use std::sync::Arc;
use regex::Regex;

/// A smart constructor validating a deflate compression level
#[derive(Clone, Debug)]
pub struct NSQDeflateLevel {
    pub(crate) level: u8
}

impl NSQDeflateLevel {
    /// Compression level N must be > 0 && < 10
    pub fn new(level: u8) -> Option<NSQDeflateLevel> {
        if level < 1 || level > 9 {
            None
        } else {
            Some(NSQDeflateLevel { level })
        }
    }

    /// Return the compression level
    pub fn get(&self) -> u8 {
        self.level
    }
}

/// NSQD TLS encryption options
#[derive(Clone)]
pub struct NSQConfigSharedTLS {
    pub(crate) required:      bool,
    pub(crate) client_config: Option<Arc<rustls::ClientConfig>>,
    pub(crate) domain_name:   String,
}

impl NSQConfigSharedTLS {
    /// Construct a TLS configuration object. Defaults are insecure.
    pub fn new<S: Into<String>>(domain: S) -> Self {
        NSQConfigSharedTLS {
            required:      true,
            client_config: None,
            domain_name:   domain.into(),
        }
    }

    /// If the connection should fail if TLS is not supported. Defaults to true.
    pub fn set_required(mut self, required: bool) -> Self {
        self.required = required;

        self
    }

    /// Set TLS configuration object from `rustls` crate.
    pub fn set_client_config(mut self, client_config: Arc<rustls::ClientConfig>) -> Self {
        self.client_config = Some(client_config);

        self
    }
}

/// NSQD TCP compression options
#[derive(Debug, Clone)]
pub enum NSQConfigSharedCompression {
    /// Use deflate compression at deflate level
    Deflate(NSQDeflateLevel),
    /// Use snappy compression
    Snappy
}

/// Configuration options shared by both produces and consumers
#[derive(Clone)]
pub struct NSQConfigShared {
    pub(crate) backoff_max_wait:      std::time::Duration,
    pub(crate) backoff_healthy_after: std::time::Duration,
    pub(crate) compression:           Option<NSQConfigSharedCompression>,
    pub(crate) tls:                   Option<NSQConfigSharedTLS>,
    pub(crate) credentials:           Option<Vec<u8>>,
    pub(crate) client_id:             Option<String>,
    pub(crate) write_timeout:         Option<std::time::Duration>,
    pub(crate) read_timeout:          Option<std::time::Duration>,
    pub(crate) hostname:              Option<String>,
    pub(crate) user_agent:            Option<String>,
}

impl NSQConfigShared {
    /// Construct a configuration with sane defaults.
    pub fn new() -> Self {
        NSQConfigShared {
            backoff_max_wait:      std::time::Duration::new(60, 0),
            backoff_healthy_after: std::time::Duration::new(45, 0),
            compression:           None,
            tls:                   None,
            credentials:           None,
            client_id:             None,
            write_timeout:         Some(std::time::Duration::new(10, 0)),
            read_timeout:          Some(std::time::Duration::new(60, 0)),
            hostname:              None,
            user_agent:            None,
        }
    }

    /// The maximum reconnect backoff wait. Defaults to 60 seconds.
    pub fn set_backoff_max_wait(mut self, duration: std::time::Duration) -> Self {
        self.backoff_max_wait = duration;

        self
    }

    /// How long a connection should be healthy before backoff is reset. Defaults to 45 seconds.
    pub fn set_backoff_healthy_after(mut self, duration: std::time::Duration) -> Self {
        self.backoff_healthy_after = duration;

        self
    }

    /// Connection compression options. Defaults to no compression.
    pub fn set_compression(mut self, compression: NSQConfigSharedCompression) -> Self {
        self.compression = Some(compression);

        self
    }

    /// Credentials to send NSQD if authentication is requried. Defaults to no credentials.
    pub fn set_credentials(mut self, credentials: Vec<u8>) -> Self {
        self.credentials = Some(credentials);

        self
    }

    /// Connection encryption options. Defaults to no encryption
    pub fn set_tls(mut self, tls: NSQConfigSharedTLS) -> Self {
        self.tls = Some(tls);

        self
    }

    /// A string used to identify an NSQ client. Defaults to anonymous identity.
    pub fn set_client_id<S: Into<String>>(mut self, client_id: S) -> Self {
        self.client_id = Some(client_id.into());

        self
    }

    /// Timeout for socket write operations. Defaults to 10 seconds.
    /// Setting the duration to `None` disables write timeouts.
    pub fn set_write_timeout(mut self, duration: Option<std::time::Duration>) -> Self {
        self.write_timeout = duration;

        self
    }

    /// Timeout for socket read operations. Defaults to 60 seconds. Must be greater than the
    /// heartbeat interval. Setting the duration to `None` disables read timeouts.
    pub fn set_read_timeout(mut self, duration: Option<std::time::Duration>) -> Self {
        self.read_timeout = duration;

        self
    }

    /// The hostname sent to NSQD. Defaults to the hostname provided by the operating system.
    pub fn set_hostname<S: Into<String>>(mut self, hostname: S) -> Self {
        self.hostname = Some(hostname.into());

        self
    }

    /// The user agent sent to NSQD. Defaults to "tokio_nsq/<version>".
    pub fn set_user_agent<S: Into<String>>(mut self, user_agent: S) -> Self {
        self.user_agent = Some(user_agent.into());

        self
    }
}

impl Default for NSQConfigShared {
    fn default() -> Self {
        Self::new()
    }
}

lazy_static! {
    static ref NAMEREGEX: Regex = Regex::new(r"^[\.a-zA-Z0-9_-]+(#ephemeral)?$").unwrap();
}

fn is_valid_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }

    NAMEREGEX.is_match(name)
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
                topic
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
                channel
            }))
        } else {
            None
        }
    }
}

/// A smart constructor validating an NSQ sample rate
#[derive(Clone, Debug, Copy)]
pub struct NSQSampleRate {
    pub(crate) rate: u8
}

impl NSQSampleRate {
    /// N must be > 0 && <= 100
    pub fn new(rate: u8) -> Option<NSQSampleRate> {
        if rate < 1 || rate > 100 {
            None
        } else {
            Some(NSQSampleRate { rate })
        }
    }
    /// Return the sample rate
    pub fn get(&self) -> u8 {
        self.rate
    }
}

#[derive(Clone)]
pub struct NSQDConfig {
    pub address:            String,
    pub subscribe:          Option<(Arc<NSQTopic>, Arc<NSQChannel>)>,
    pub shared:             NSQConfigShared,
    pub sample_rate:        Option<NSQSampleRate>,
    pub max_requeue_delay:  std::time::Duration,
    pub base_requeue_delay: std::time::Duration,
}
