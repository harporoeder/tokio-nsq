#[derive(Debug, Clone)]
pub struct NSQConfigSharedTLS {
    required: bool
}

impl NSQConfigSharedTLS {
    pub fn new() -> Self {
        return NSQConfigSharedTLS {
            required: true,
        }
    }

    pub fn set_required(mut self, required: bool) -> Self {
        self.required = required;

        return self;
    }
}

#[derive(Debug, Clone)]
pub enum NSQConfigSharedCompression {
    Deflate(u8)
}

#[derive(Debug, Clone)]
pub struct NSQConfigShared {
    pub backoff_max_wait:      std::time::Duration,
    pub backoff_healthy_after: std::time::Duration,
    pub compression:           Option<NSQConfigSharedCompression>,
    pub tls:                   Option<NSQConfigSharedTLS>,
}

impl NSQConfigShared {
    pub fn new() -> Self {
        return NSQConfigShared {
            backoff_max_wait:      std::time::Duration::new(60, 0),
            backoff_healthy_after: std::time::Duration::new(45, 0),
            compression:           None,
            tls:                   None,
        }
    }

    pub fn set_backoff_max_wait(mut self, duration: std::time::Duration) -> Self {
        self.backoff_max_wait = duration;

        return self;
    }

    pub fn set_backoff_healthy_after(mut self, duration: std::time::Duration) -> Self {
        self.backoff_healthy_after = duration;

        return self;
    }

    pub fn set_compression(mut self, compression: NSQConfigSharedCompression) -> Self {
        self.compression = Some(compression);

        return self;
    }

    pub fn set_tls(mut self, tls: NSQConfigSharedTLS) -> Self {
        self.tls = Some(tls);

        return self;
    }
}
