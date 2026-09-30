use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[derive(Debug)]
pub struct Metrics {
    active_connections: AtomicUsize,
    requests_received: AtomicUsize,
    command_responses_written: AtomicUsize,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    command_parse_errors: AtomicUsize,
    accept_errors: AtomicUsize,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            requests_received: AtomicUsize::new(0),
            command_responses_written: AtomicUsize::new(0),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            command_parse_errors: AtomicUsize::new(0),
            accept_errors: AtomicUsize::new(0),
        }
    }

    /// Count Current number of handlers whose `run()` guard is alive.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Relaxed)
    }

    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Relaxed);
    }

    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Relaxed);
    }

    /// Count complete RESP frames passed to command parsing,
    /// including frames containing invalid commands
    /// Exclude malformed frames.
    pub fn requests_received(&self) -> usize {
        self.requests_received.load(Relaxed)
    }

    pub fn inc_requests_received(&self) {
        self.requests_received.fetch_add(1, Relaxed);
    }

    /// Count Successful response writes from command processing,
    /// including command-error replies.
    /// Excludes replies from the frame/read-error branch.
    pub fn command_responses_written(&self) -> usize {
        self.command_responses_written.load(Relaxed)
    }

    pub fn inc_command_responses_written(&self) {
        self.command_responses_written.fetch_add(1, Relaxed);
    }

    /// Count GET lookups that found a value, regardless of response-write success.
    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(Relaxed)
    }

    pub fn inc_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Relaxed);
    }

    /// Count GET lookups that found no value, regardless of response-write success.
    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(Relaxed)
    }

    pub fn inc_cache_misses(&self) {
        self.cache_misses.fetch_add(1, Relaxed);
    }

    /// Count Complete frames rejected by `Command::from_frame`.
    pub fn command_parse_errors(&self) -> usize {
        self.command_parse_errors.load(Relaxed)
    }

    pub fn inc_command_parse_errors(&self) {
        self.command_parse_errors.fetch_add(1, Relaxed);
    }

    /// Count Errors returned by `listener.accept()`.
    /// Waiting at the connection limit does not count.
    pub fn accept_errors(&self) -> usize {
        self.accept_errors.load(Relaxed)
    }

    pub fn inc_accept_errors(&self) {
        self.accept_errors.fetch_add(1, Relaxed);
    }
}
