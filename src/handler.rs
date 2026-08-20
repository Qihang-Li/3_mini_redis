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

#[derive(Debug)]
pub struct Handler {
    connection: Connection,
    database: Database,
    broadcast_rx: broadcast::Receiver<()>,
    _mpsc_tx: mpsc::Sender<()>,
    _permit: OwnedSemaphorePermit,
    timeout_duration: Duration,
    metrics: Arc<Metrics>,
}

impl Handler {
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

    /// Executes a per-client event loop continuously.
    ///
    /// # Errors
    /// Returns an error if a fatal network boundary is breached, during
    /// transmission, or if the connection times out.
    pub async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: increment `active_connections` by 1
        self.metrics.inc_active_connections();

        // Step 1: start an infinite event loop for client commands continuously
        loop {
            // Step 2: use `tokio::select!` for the race
            tokio::select! {
                // 2.(i) the business logic comes first
                timeout_result = timeout(self.timeout_duration, self.connection.read_frame()) => {

                    // Step 3: match `timeout_result` to unwrap the timeout wrapper
                    let read_frame_result = match timeout_result {
                        // 3.(i) the business logic comes first
                        Ok(result) => result,
                        // 3.(ii) the timeout comes first
                        Err(_elapsed) => {
                            tracing::info!("Timeout error! Server idle for too long");
                            break;
                        },
                    };

                    // Step 4: match `read_frame_result` to unpack `connection::read_frame()` results
                    let input_frame = match read_frame_result {
                        // 4.(i) the happy path with a `Frame`
                        Ok(Some(frame)) => frame,
                        // 4.(ii) the graceful disconnect
                        Ok(None) => {
                            tracing::info!("The client disconnected");
                            break;
                        },
                        // 4.(iii) the error when client disconnected
                        Err(error) => {
                            // construct the error payload
                            let error_frame = Frame::Error(error.to_string());
                            // attempt a best-effort transmission to the client
                            let _ = self.connection.write_frame(&error_frame).await;
                            // drop the connection to protect the server
                            return Err(error);
                        }
                    };

                    // Step 5: Try to get a `Command`
                    let output_frame = match Command::from_frame(input_frame) {
                        // 5.(i) the happy path with a valid `Command`

                        // Step 6: Execute the `Command`
                        // 6.(i) a get command
                        Ok(Command::Get(command)) => {
                            let frame = Command::Get(command).apply(&self.database);
                            match frame {
                                // increment `cache_hits` by 1
                                Frame::Bulk(_) => self.metrics.inc_cache_hits(),
                                // increment `cache_misses` by 1
                                Frame::Null => self.metrics.inc_cache_misses(),
                                _ => {} // Ignore any other unexpected states
                            }
                            frame // Return the frame to the outer assignment
                        },
                        // 6.(ii) a set command
                        Ok(Command::Set(command)) => Command::Set(command).apply(&self.database),

                        // 5.(ii) input_frame can't form a valid `Command`
                        Err(_) => {
                            // increment `parse_failures` by 1
                            self.metrics.inc_parse_failures();
                            Frame::Error("Wrong message: not a valid command".to_string())
                        }
                    };

                    // Step 7: formating `output_frame` into a response
                    self.connection.write_frame(&output_frame).await?;

                    // Step 8: increment `total_requests` by 1
                    self.metrics.inc_total_requests();
                },
                // 2.(ii) server shutdown signal comes first
                _ = self.broadcast_rx.recv() => {
                    tracing::info!("Server shutdown signal received.");
                    break;
                },
            };
        }

        // Step 9: decrement `active_connections` by 1
        self.metrics.dec_active_connections();
        // Step 10: end of the event loop
        Ok(())
    }
}
