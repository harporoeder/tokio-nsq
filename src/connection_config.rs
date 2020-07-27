/// NSQD TLS encryption options
#[derive(Debug, Clone)]
pub struct NSQConfigSharedTLS {
    required: bool
}

impl NSQConfigSharedTLS {
    /// Construct a TLS configuration object. Defaults are insecure.
    pub fn new() -> Self {
        return NSQConfigSharedTLS {
            required: true,
        }
    }
    /// If the connection should fail if TLS is not supported. Defaults to true.
    pub fn set_required(mut self, required: bool) -> Self {
        self.required = required;

        return self;
    }
}

/// NSQD TCP compression options
#[derive(Debug, Clone)]
pub enum NSQConfigSharedCompression {
    Deflate(u8)
}

/// Configuration options shared by both produces and consumers
#[derive(Debug, Clone)]
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
        return NSQConfigShared {
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

        return self;
    }
    /// How long a connection should be healthy before backoff is reset. Defaults to 45 seconds.
    pub fn set_backoff_healthy_after(mut self, duration: std::time::Duration) -> Self {
        self.backoff_healthy_after = duration;

        return self;
    }
    /// Connection compression options. Defaults to no compression.
    pub fn set_compression(mut self, compression: NSQConfigSharedCompression) -> Self {
        self.compression = Some(compression);

        return self;
    }
    /// Credentials to send NSQD if authentication is requried. Defaults to no credentials.
    pub fn set_credentials(mut self, credentials: Vec<u8>) -> Self {
        self.credentials = Some(credentials);

        return self;
    }
    /// Connection encryption options. Defaults to no encryption
    pub fn set_tls(mut self, tls: NSQConfigSharedTLS) -> Self {
        self.tls = Some(tls);

        return self;
    }
    /// A string used to identify an NSQ client. Defaults to anonymous identity.
    pub fn set_client_id<S: Into<String>>(mut self, client_id: S) -> Self {
        self.client_id = Some(client_id.into());

        return self;
    }
}
