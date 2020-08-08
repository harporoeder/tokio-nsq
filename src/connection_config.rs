use std::sync::Arc;

/// A smart constructor validating a deflate compression level
#[derive(Clone, Debug)]
pub struct NSQDeflateLevel {
    pub(crate) level: u8
}

impl NSQDeflateLevel {
    /// Compression level must be > 0 && n < 10
    pub fn new(level: u8) -> Option<NSQDeflateLevel> {
        if level < 1 || level > 9 {
            None
        } else {
            Some(NSQDeflateLevel { level })
        }
    }
}

/// NSQD TLS encryption options
#[derive(Clone)]
pub struct NSQConfigSharedTLS {
    pub(crate) required:      bool,
    pub(crate) client_config: Option<Arc<rustls::ClientConfig>>,
}

impl NSQConfigSharedTLS {
    /// Construct a TLS configuration object. Defaults are insecure.
    pub fn new() -> Self {
        NSQConfigSharedTLS {
            required:      true,
            client_config: None,
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

impl Default for NSQConfigSharedTLS {
    fn default() -> Self {
        Self::new()
    }
}

/// NSQD TCP compression options
#[derive(Debug, Clone)]
pub enum NSQConfigSharedCompression {
    Deflate(NSQDeflateLevel)
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
}

impl Default for NSQConfigShared {
    fn default() -> Self {
        Self::new()
    }
}
