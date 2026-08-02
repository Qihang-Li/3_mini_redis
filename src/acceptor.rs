use crate::database::Database;
use std::error::Error;
use std::sync::Arc;
use std::unimplemented;
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
}

impl Acceptor {
    pub fn new(
        listener: TcpListener,
        database: Database,
        broadcast_tx: broadcast::Sender<()>,
        mpsc_tx: mpsc::Sender<()>,
        max_connections: usize,
    ) -> Self {
        Self {
            listener,
            database,
            broadcast_tx,
            mpsc_tx,
            semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Runs an infinite loop as a router for main business logic
    ///
    /// # Errors
    /// Returns an error if network stack of OS crashes,
    /// or if a fatal OS resource limit is breached.
    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // Step 1: set up the broadcast receiver for Listener
        let mut broadcast_rx_listener = self.broadcast_tx.subscribe();

        // Step 2: start an infinite loop and prepare semaphore inside
        loop {
            let semaphore_clone = self.semaphore.clone();
            let permit = semaphore_clone.acquire_owned().await?;

            // Step 3: use `tokio::select!` for the race
            tokio::select! {
                result = self.listener.accept() => {

                    // Step 4: match the `result`
                    match result {

                        // 4.1 The main business logic
                        Ok((socket, _)) => {

                            // Step 5: prepare tools for the spawned task
                            let db_cloned = self.database.clone();
                            let broadcast_rx_handler = self.broadcast_tx.subscribe();
                            let mpsc_tx_cloned = self.mpsc_tx.clone();

                            let handle = tokio::spawn(async move {

                                // Step 6: to be continued
                                unimplemented!();
                            });
                        },

                        // 4.2 wait if listener.accept() gets an io_error
                        Err(error) => {
                            match error.kind() {
                                // Silently ignore transient client disconnections
                                std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::ConnectionReset => {},
                                // Log and sleep on all other errors (like OS resource exhaustion)
                                _ => {
                                    tracing::error!(%error, "failed to accept connection");
                                    sleep(Duration::from_millis(50)).await;
                                }
                            }
                        }
                    }
                },

                // quit the loop once the shutdown signal is received
                _ = broadcast_rx_listener.recv() => {
                    return Ok(())
                }
            };
        }
    }
}
