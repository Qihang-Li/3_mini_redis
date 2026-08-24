use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[derive(Debug)]
pub struct Metrics {
    active_connections: AtomicUsize,
    total_requests: AtomicUsize,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    parse_failures: AtomicUsize,
    rejected_connections: AtomicUsize,
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
            rejected_connections: AtomicUsize::new(0),
            total_requests: AtomicUsize::new(0),
            parse_failures: AtomicUsize::new(0),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
        }
    }

    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Relaxed);
    }

    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Relaxed);
    }

    pub fn inc_rejected_connections(&self) {
        self.rejected_connections.fetch_add(1, Relaxed);
    }

    pub fn inc_total_requests(&self) {
        self.total_requests.fetch_add(1, Relaxed);
    }

    pub fn inc_parse_failures(&self) {
        self.parse_failures.fetch_add(1, Relaxed);
    }

    pub fn inc_cache_hits(&self) {
        self.cache_hits.fetch_add(1, Relaxed);
    }

    pub fn inc_cache_misses(&self) {
        self.cache_misses.fetch_add(1, Relaxed);
    }

    pub fn total_requests(&self) -> usize {
        self.total_requests.load(Relaxed)
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Relaxed)
    }
}
