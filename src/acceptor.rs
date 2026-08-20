use crate::database::Database;
use crate::handler::Handler;
use crate::metrics::Metrics;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast, mpsc};
use tokio::time::{Duration, sleep};

#[derive(Debug)]
pub struct Acceptor {
    listener: TcpListener,
    database: Database,
    broadcast_tx: broadcast::Sender<()>,
    mpsc_tx: mpsc::Sender<()>,
    semaphore: Arc<Semaphore>,
    timeout_duration: Duration,
    metrics: Arc<Metrics>,
}

impl Acceptor {
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

    /// Runs an infinite loop as a router for main business logic
    ///
    /// # Errors
    /// Returns an error if network stack of OS crashes,
    /// or if a fatal OS resource limit is breached.
    pub async fn run(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: set up the broadcast receiver for Listener
        let mut broadcast_rx_acceptor = self.broadcast_tx.subscribe();

        // Step 2: start an infinite loop and prepare semaphore inside
        loop {
            let permit = self.semaphore.clone().acquire_owned().await?;

            // Step 3: use `tokio::select!` for the race
            tokio::select! {
                // 3.(i) the business logic comes first
                result = self.listener.accept() => {

                    // Step 4: match the `result`
                    match result {
                        // 4.(i) The main business logic
                        Ok((socket, _)) => {

                            // Step 5: prepare tools for the spawned task
                            let db_clone = self.database.clone();
                            let broadcast_rx_handler = self.broadcast_tx.subscribe();
                            let mpsc_tx_clone = self.mpsc_tx.clone();
                            let timeout_duration = self.timeout_duration;
                            let metrics = self.metrics.clone();
                            let _handle = tokio::spawn(async move {

                                // Step 6: execute the business logic by `handler`
                                let handler = Handler::new(
                                    socket,
                                    db_clone,
                                    broadcast_rx_handler,
                                    mpsc_tx_clone,
                                    permit,
                                    timeout_duration,
                                    metrics,
                                );
                                // await the future and evaluate the Result
                                if let Err(error) = handler.run().await {
                                    tracing::error!(%error, "Handler failed to execute");
                                }
                            });
                        },
                        // 4.(ii) wait if listener.accept() gets an io_error
                        Err(error) => {
                            // increment `rejected_connections` by 1
                            self.metrics.inc_rejected_connections();
                            match error.kind() {
                                // ignore transient client disconnections silently
                                std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset => {},
                                // log and sleep on all other errors (like OS resource exhaustion)
                                _ => {
                                    tracing::error!(%error, "failed to accept connection");
                                    sleep(Duration::from_millis(50)).await;
                                }
                            }
                        }
                    }
                },
                // 3.(ii) the server shutdown signal comes first
                _ = broadcast_rx_acceptor.recv() => {
                    // quit the loop once the shutdown signal is received
                    tracing::info!("Shutdown signal received from OS");
                    break;
                }
            };
        }

        Ok(())
    }
}
