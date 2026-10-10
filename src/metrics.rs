use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Stores the current active connection count and cumulative event counts.
///
/// Each field uses atomic operations with `Relaxed` ordering. These operations
/// do not synchronize other shared state. Reading several fields separately
/// does not guarantee a snapshot from a single instant.
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
    /// Creates metrics with every value initialized to zero.
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

    /// Returns the number of handlers with a live `run()` guard.
    ///
    /// The guard is created when `Handler::run()` starts executing.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Relaxed)
    }

    /// Increments `active_connections` by one.
    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Relaxed);
    }

    /// Decrements `active_connections` by one.
    ///
    /// The handler's guard calls this to balance its earlier increment.
    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Relaxed);
    }

    /// Returns the number of complete RESP frames passed to command parsing.
    ///
    /// Includes frames containing invalid commands. Excludes input rejected
    /// during frame reading.
    pub fn requests_received(&self) -> usize {
        self.requests_received.load(Relaxed)
    }

    /// Increments `requests_received` by one.
    pub fn inc_requests_received(&self) {
        self.requests_received.fetch_add(1, Relaxed);
    }

    /// Returns the number of successfully written command responses.
    ///
    /// The count increases after `write_frame()` succeeds. Includes replies to
    /// invalid commands. Excludes replies from the frame-reading error branch.
    ///
    /// A successful write does not confirm that the client read the response.
    pub fn command_responses_written(&self) -> usize {
        self.command_responses_written.load(Relaxed)
    }

    /// Increments `command_responses_written` by one.
    pub fn inc_command_responses_written(&self) {
        self.command_responses_written.fetch_add(1, Relaxed);
    }

    /// Returns the number of GET lookups that found a value.
    ///
    /// Includes lookups whose response could not be written.
    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(Relaxed)
    }

    /// Increments `cache_hits` by one.
    pub fn inc_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Relaxed);
    }

    /// Returns the number of GET lookups that found no value.
    ///
    /// Includes lookups whose response could not be written.
    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(Relaxed)
    }

    /// Increments `cache_misses` by one.
    pub fn inc_cache_misses(&self) {
        self.cache_misses.fetch_add(1, Relaxed);
    }

    /// Returns the number of complete frames rejected by `Command::from_frame`.
    ///
    /// Excludes malformed frames rejected before command parsing.
    pub fn command_parse_errors(&self) -> usize {
        self.command_parse_errors.load(Relaxed)
    }

    /// Increments `command_parse_errors` by one.
    pub fn inc_command_parse_errors(&self) {
        self.command_parse_errors.fetch_add(1, Relaxed);
    }

    /// Returns the number of errors returned by `listener.accept()`.
    ///
    /// Includes errors that are retried without logging. Waiting for a
    /// connection permit does not increment this count.
    pub fn accept_errors(&self) -> usize {
        self.accept_errors.load(Relaxed)
    }

    /// Increments `accept_errors` by one.
    pub fn inc_accept_errors(&self) {
        self.accept_errors.fetch_add(1, Relaxed);
    }
}
