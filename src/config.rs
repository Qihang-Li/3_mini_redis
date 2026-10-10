use std::time::Duration;
use tracing::Level;

pub const DEFAULT_MAX_CONNECTIONS: usize = 512;

/// Capacity of the shutdown channel's message buffer.
///
/// This limits retained messages, not the number of receivers.
pub const SHUTDOWN_BROADCAST_CAPACITY: usize = 512;

/// Initial capacity requested for each connection's read buffer.
/// The buffer may grow as more data arrives.
pub const INITIAL_READ_BUFFER_CAPACITY: usize = 4096;

/// Maximum unread bytes buffered by a connection.
///
/// This bounds buffer length, not allocated capacity. A complete frame may
/// occupy the full budget.
pub const MAX_BUFFERED_BYTES: usize = 65536;

/// Exclusive upper bound on the element count of each RESP array.
pub const ARRAY_LENGTH_LIMIT_EXCLUSIVE: i64 = 1024;

/// Maximum array layers along one nesting path, including the outermost array.
pub const MAX_ARRAY_DEPTH: i32 = 32;

pub const DEFAULT_SERVER_BIND_ADDR: &str = "127.0.0.1:6379";
pub const DEFAULT_CLIENT_ADDR: &str = "127.0.0.1:6379";

/// Default timeout for reading one complete frame, including partial reads.
/// Receiving more bytes does not restart this timeout.
pub const DEFAULT_SERVER_READ_TIMEOUT: Duration = Duration::from_mins(10);

/// Default timeout applied separately to connecting, writing a request,
/// and reading a response.
pub const DEFAULT_CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Delay before retrying an accept error other than connection abort or reset.
pub const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(50);

pub const DEFAULT_LOG_LEVEL: Level = Level::INFO;
