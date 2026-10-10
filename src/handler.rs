use crate::command::Command;
use crate::connection::Connection;
use crate::database::Database;
use crate::frame::Frame;
use crate::metrics::Metrics;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, broadcast, mpsc};
use tokio::time::{Duration, timeout};

/// Counts one running handler until the guard is dropped.
#[derive(Debug)]
struct ActiveConnectionGuard {
    metrics: Arc<Metrics>,
}

impl ActiveConnectionGuard {
    /// Increments the active count and creates its matching cleanup guard.
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.inc_active_connections();
        Self { metrics }
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.metrics.dec_active_connections();
    }
}

/// Processes commands received over one client connection.
#[derive(Debug)]
pub struct Handler {
    connection: Connection,
    database: Database,
    broadcast_rx: broadcast::Receiver<()>,
    // Holds a sender for shutdown tracking; no messages are sent here.
    _mpsc_tx: mpsc::Sender<()>,
    // Holds one connection slot until the handler is dropped.
    _permit: OwnedSemaphorePermit,
    // Applies to each frame read, including collection of partial input.
    timeout_duration: Duration,
    metrics: Arc<Metrics>,
}

impl Handler {
    /// Creates a handler that owns the connection and its semaphore permit.
    ///
    /// Keeps `_mpsc_tx` alive for shutdown tracking. `timeout_duration` applies
    /// to each call that reads a complete frame.
    ///
    /// The active connection count increases when `run()` starts executing.
    #[allow(clippy::used_underscore_binding)]
    pub fn new(
        stream: TcpStream,
        database: Database,
        broadcast_rx: broadcast::Receiver<()>,
        _mpsc_tx: mpsc::Sender<()>,
        _permit: OwnedSemaphorePermit,
        timeout_duration: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            connection: Connection::new(stream),
            database,
            broadcast_rx,
            _mpsc_tx,
            _permit,
            timeout_duration,
            metrics,
        }
    }

    /// Processes requests for one client connection.
    ///
    /// Applies a fresh timeout to each `read_frame()` call. Receiving partial
    /// data does not restart the timeout. Response writes have no timeout, and
    /// the shutdown receiver is not polled during those writes.
    ///
    /// Returns `Ok(())` after clean EOF, a read timeout, or selection of the
    /// shutdown receive branch. Broadcast receive errors also select that
    /// branch.
    ///
    /// Invalid commands produce an error response. Processing continues if
    /// that response is written successfully.
    ///
    /// # Errors
    ///
    /// Returns frame-reading errors, including invalid input, exceeded input
    /// limits, incomplete input at EOF, and I/O failures. Before returning a
    /// read error, attempts an error response and ignores its write result.
    ///
    /// Also returns errors encountered while writing command responses.
    pub async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: Track this running handler
        // Dropping the guard decrements the count on normal or error returns.
        // Cancelling and dropping this running future also drops the guard.
        let _active_connection_guard = ActiveConnectionGuard::new(self.metrics.clone());

        // Step 1: Process successive requests from this connection
        loop {
            // Step 2: Wait for a frame read, its timeout, or shutdown
            tokio::select! {
                // Frame read or timeout completed
                timeout_result = timeout(self.timeout_duration, self.connection.read_frame()) => {

                    // Step 3: Separate the read result from a timeout
                    let read_frame_result = match timeout_result {
                        // Read completed with a frame, EOF, or error
                        Ok(result) => result,
                        // Read timeout
                        Err(_elapsed) => {
                            tracing::info!("Timeout error! Server idle for too long");
                            break;
                        },
                    };

                    // Step 4: Interpret the frame-read result
                    let input_frame = match read_frame_result {
                        // Complete frame
                        Ok(Some(frame)) => frame,
                        // EOF with no incomplete frame buffered
                        Ok(None) => {
                            tracing::info!("The client disconnected");
                            break;
                        },
                        // Read, framing, or input-limit error
                        Err(error) => {
                            // Build a reply describing the read error.
                            let error_frame = Frame::Error(error.to_string());
                            // Attempt the reply; ignore any write error.
                            // This write is awaited without a timeout.
                            let _ = self.connection.write_frame(&error_frame).await;
                            // Return the original read error.
                            return Err(error);
                        }
                    };

                    // Step 5: Count the frame before command validation
                    self.metrics.inc_requests_received();

                    // Step 6: Parse and execute the command
                    let output_frame = match Command::from_frame(input_frame) {
                        // GET command
                        Ok(Command::Get(command)) => {
                            let frame = Command::Get(command).apply(&self.database);
                            match frame {
                                // Count the lookup before writing its response.
                                Frame::Bulk(_) => self.metrics.inc_cache_hits(),
                                // Count the missing key before writing.
                                Frame::Null => self.metrics.inc_cache_misses(),
                                // Leave other response types uncounted here.
                                _ => {}
                            }
                            // Use this lookup result as the response.
                            frame
                        },
                        // SET command
                        Ok(Command::Set(command)) => Command::Set(command).apply(&self.database),

                        // Complete frame containing an invalid command
                        Err(_) => {
                            // Count the invalid command before replying.
                            self.metrics.inc_command_parse_errors();
                            Frame::Error("Wrong message: not a valid command".to_string())
                        }
                    };

                    // Step 7: Write and flush the command response
                    self.connection.write_frame(&output_frame).await?;

                    // Step 8: Count the successful command-response write
                    // This includes replies to invalid commands.
                    self.metrics.inc_command_responses_written();
                },
                // Shutdown message or broadcast receive error
                _ = self.broadcast_rx.recv() => {
                    tracing::info!("Server shutdown signal received.");
                    break;
                },
            };
        }
        // EOF, read timeout, and shutdown end the loop successfully.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::{Semaphore, broadcast, mpsc};

    #[tokio::test]
    async fn test_handler_read_error_releases_active_connection()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: Set up a connected client and server socket
        // Bind a local listener on an OS-assigned port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the assigned address for the client connection.
        let listener_addr = listener.local_addr().unwrap();
        // Connect the client-side TCP stream.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server-side TCP stream.
        let (server, _) = listener.accept().await?;
        // Share metrics between the handler and the assertions.
        let metrics = Arc::new(Metrics::new());
        // Keep the shutdown sender alive for the duration of the test.
        let (_broadcast_tx, broadcast_rx) =
            broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);

        // Create a handler with one permit and an isolated database.
        let handler = Handler::new(
            server,
            Database::new(),
            broadcast_rx,
            mpsc::channel::<()>(1).0,
            Arc::new(Semaphore::new(config::DEFAULT_MAX_CONNECTIONS))
                .clone()
                .acquire_owned()
                .await?,
            config::DEFAULT_SERVER_READ_TIMEOUT,
            Arc::clone(&metrics),
        );

        // Step 1: Send an invalid integer frame
        client.write_all(":a\r\n".as_bytes()).await?;
        // Step 2: Run the handler with a one-second test timeout
        let result = tokio::time::timeout(Duration::from_secs(1), handler.run()).await?;
        // Step 3: Check the result, request counters, and cleanup
        assert!(result.is_err());
        assert_eq!(metrics.active_connections(), 0);
        assert_eq!(metrics.requests_received(), 0);
        assert_eq!(metrics.command_responses_written(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_handler_eof_releases_active_connection()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: Set up a connected client and server socket
        // Bind a local listener on an OS-assigned port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the assigned address for the client connection.
        let listener_addr = listener.local_addr().unwrap();
        // Connect the client-side TCP stream.
        let client = TcpStream::connect(listener_addr).await?;
        // Accept the server-side TCP stream.
        let (server, _) = listener.accept().await?;
        // Share metrics between the handler and the assertions.
        let metrics = Arc::new(Metrics::new());
        // Keep the shutdown sender alive for the duration of the test.
        let (_broadcast_tx, broadcast_rx) =
            broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);

        // Create a handler with one permit and an isolated database.
        let handler = Handler::new(
            server,
            Database::new(),
            broadcast_rx,
            mpsc::channel::<()>(1).0,
            Arc::new(Semaphore::new(config::DEFAULT_MAX_CONNECTIONS))
                .clone()
                .acquire_owned()
                .await?,
            config::DEFAULT_SERVER_READ_TIMEOUT,
            Arc::clone(&metrics),
        );

        // Step 1: Close the client without sending data
        drop(client);
        // Step 2: Run the handler with a one-second test timeout
        let result = tokio::time::timeout(Duration::from_secs(1), handler.run()).await?;
        // Step 3: Check the result, request counters, and cleanup
        assert!(result.is_ok());
        assert_eq!(metrics.requests_received(), 0);
        assert_eq!(metrics.command_responses_written(), 0);
        assert_eq!(metrics.active_connections(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_handler_write_error_increments_request_metrics()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: Set up a connected client and server socket
        // Bind a local listener on an OS-assigned port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the assigned address for the client connection.
        let listener_addr = listener.local_addr().unwrap();
        // Connect the client-side TCP stream.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server-side TCP stream.
        let (mut server, _) = listener.accept().await?;
        // Disable server writes so the response attempt fails.
        // The server can still read the incoming request.
        server.shutdown().await?;
        // Share metrics between the handler and the assertions.
        let metrics = Arc::new(Metrics::new());
        // Keep the shutdown sender alive for the duration of the test.
        let (_broadcast_tx, broadcast_rx) =
            broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);

        // Create a handler with one permit and an isolated database.
        let handler = Handler::new(
            server,
            Database::new(),
            broadcast_rx,
            mpsc::channel::<()>(1).0,
            Arc::new(Semaphore::new(config::DEFAULT_MAX_CONNECTIONS))
                .clone()
                .acquire_owned()
                .await?,
            config::DEFAULT_SERVER_READ_TIMEOUT,
            Arc::clone(&metrics),
        );

        // Step 1: Send a valid GET request
        // The server can read the request, but its write half is shut down.
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await?;
        // Step 2: Run the handler with a one-second test timeout
        let result = tokio::time::timeout(Duration::from_secs(1), handler.run()).await?;
        // Step 3: Check the result, request counters, and cleanup
        assert!(result.is_err());
        assert_eq!(metrics.requests_received(), 1);
        assert_eq!(metrics.command_responses_written(), 0);
        assert_eq!(metrics.active_connections(), 0);

        Ok(())
    }
}
