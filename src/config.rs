use std::time::Duration;
use tracing::Level;

pub const DEFAULT_MAX_CONNECTIONS: usize = 512;
pub const SHUTDOWN_BROADCAST_CAPACITY: usize = 512;

pub const INITIAL_READ_BUFFER_CAPACITY: usize = 4096;
pub const MAX_BUFFERED_BYTES: usize = 65536;

pub const ARRAY_LENGTH_LIMIT_EXCLUSIVE: i64 = 1024;
pub const MAX_ARRAY_DEPTH: i32 = 32;

pub const DEFAULT_SERVER_BIND_ADDR: &str = "127.0.0.1:6379";
pub const DEFAULT_CLIENT_ADDR: &str = "127.0.0.1:6379";

pub const DEFAULT_SERVER_READ_TIMEOUT: Duration = Duration::from_mins(10);
pub const DEFAULT_CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(10);
pub const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(50);

pub const DEFAULT_LOG_LEVEL: Level = Level::INFO;
