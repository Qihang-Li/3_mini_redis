use crate::config;
use crate::database::Database;
use crate::handler::Handler;
use crate::metrics::Metrics;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::time::{Duration, sleep};

/// Accepts TCP connections and spawns handlers within a connection limit.
#[derive(Debug)]
pub struct Acceptor {
    listener: TcpListener,
    database: Database,
    // Creates shutdown receivers for this acceptor and its handlers.
    broadcast_tx: broadcast::Sender<()>,
    // Keeps the shutdown-completion channel open; handlers receive clones.
    mpsc_tx: mpsc::Sender<()>,
    semaphore: Arc<Semaphore>,
    // Supplies the frame-read timeout used by each handler.
    timeout_duration: Duration,
    metrics: Arc<Metrics>,
}

impl Acceptor {
    /// Creates an acceptor with shared state and a limit on handlers.
    ///
    /// Uses `max_connections` as the initial number of semaphore permits and
    /// passes `timeout_duration` to each handler as its frame-read timeout.
    /// A zero connection limit leaves `run()` waiting for its first permit.
    ///
    /// # Panics
    ///
    /// Panics if `max_connections` exceeds [`Semaphore::MAX_PERMITS`].
    pub fn new(
        listener: TcpListener,
        database: Database,
        broadcast_tx: broadcast::Sender<()>,
        mpsc_tx: mpsc::Sender<()>,
        max_connections: usize,
        timeout_duration: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            listener,
            database,
            broadcast_tx,
            mpsc_tx,
            semaphore: Arc::new(Semaphore::new(max_connections)),
            timeout_duration,
            metrics,
        }
    }

    /// Accepts client connections and spawns a handler for each one.
    ///
    /// Reserves a semaphore permit before each accept attempt. At the
    /// connection limit, waits for capacity before accepting again.
    ///
    /// Returns `Ok(())` when the shutdown receive branch is selected, including
    /// when the receive operation returns an error. This method does not wait
    /// for spawned handlers to finish.
    ///
    /// The shutdown receiver is not polled while waiting for a permit or
    /// sleeping after an accept error.
    ///
    /// # Errors
    ///
    /// Propagates a permit-acquisition error if the semaphore is closed.
    /// The current implementation does not close this semaphore.
    ///
    /// Accept errors are counted and retried. Handler errors are logged inside
    /// their spawned tasks rather than returned by this method.
    pub async fn run(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: Subscribe to shutdown notifications
        let mut broadcast_rx_acceptor = self.broadcast_tx.subscribe();

        // Step 2: Reserve a connection slot before each accept attempt
        loop {
            // Keep the permit until it is moved into a handler or dropped.
            let permit = self.semaphore.clone().acquire_owned().await?;

            // Step 3: Wait for an accept result or shutdown notification
            // Neither branch has fixed priority when both are ready.
            tokio::select! {
                // Accept operation completed
                result = self.listener.accept() => {

                    // Step 4: Handle the accept result
                    match result {
                        // Accepted connection
                        Ok((socket, _)) => {

                            // Step 5: Prepare shared handles for the task
                            let db_clone = self.database.clone();
                            let broadcast_rx_handler = self.broadcast_tx.subscribe();
                            let mpsc_tx_clone = self.mpsc_tx.clone();
                            let timeout_duration = self.timeout_duration;
                            let metrics = self.metrics.clone();
                            let _handle = tokio::spawn(async move {

                                // Step 6: Create the handler with the permit
                                // Dropping the handler releases its permit.
                                let handler = Handler::new(
                                    socket,
                                    db_clone,
                                    broadcast_rx_handler,
                                    mpsc_tx_clone,
                                    permit,
                                    timeout_duration,
                                    metrics,
                                );
                                // Run the handler and log any returned error.
                                if let Err(error) = handler.run().await {
                                    tracing::error!(%error, "Handler failed to execute");
                                }
                            });
                        },
                        // Accept failed
                        Err(error) => {
                            // Count every accept failure.
                            self.metrics.inc_accept_errors();
                            match error.kind() {
                                // Retry these errors immediately.
                                std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset => {},
                                // Log other errors and delay the next attempt.
                                _ => {
                                    tracing::error!(%error, "failed to accept connection");
                                    sleep(config::ACCEPT_RETRY_DELAY).await;
                                }
                            }
                        }
                    }
                },
                // Shutdown receive completed
                _ = broadcast_rx_acceptor.recv() => {
                    // Stop on either a message or a receive error.
                    tracing::info!("Shutdown signal received from OS");
                    break;
                }
            };
        }

        Ok(())
    }
}
