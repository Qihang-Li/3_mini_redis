use mini_redis::acceptor::Acceptor;
use mini_redis::config;
use mini_redis::database::Database;
use mini_redis::metrics::Metrics;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Step 0: Configure application logging
    // Step 0.1: Build a subscriber for formatted log output
    let subscriber = FmtSubscriber::builder()
        // Include events up to the configured maximum verbosity.
        .with_max_level(config::DEFAULT_LOG_LEVEL)
        // Finish configuring the subscriber.
        .finish();

    // Step 0.2: Install the application's global tracing subscriber
    tracing::subscriber::set_global_default(subscriber)
        // Require logging setup to succeed before starting the server.
        .expect("Failed to set tracing subscriber");

    // Step 0.3: Log the startup message
    info!("Mini-Redis Server Daemon initializing...");

    // Step 1: Initialize the shared server resources
    // Step 1.1: Create the shared database
    let db = Database::new();

    // Step 1.2: Create shutdown signaling and completion tracking
    // Broadcast capacity bounds the number of retained messages.
    let (broadcast_tx, _broadcast_rx) =
        broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);
    // Use sender lifetimes to track shutdown; no values are sent.
    // Capacity 1 is sufficient for this use.
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<()>(1);

    // Step 1.3: Bind the listener to the configured address
    let listener = TcpListener::bind(config::DEFAULT_SERVER_BIND_ADDR).await?;
    info!("Server listening on {}", config::DEFAULT_SERVER_BIND_ADDR);

    // Step 1.4: Create shared metrics and configure the acceptor
    let metrics = Arc::new(Metrics::new());
    let mut acceptor = Acceptor::new(
        listener,
        db,
        broadcast_tx.clone(),
        mpsc_tx.clone(),
        config::DEFAULT_MAX_CONNECTIONS,
        config::DEFAULT_SERVER_READ_TIMEOUT,
        Arc::clone(&metrics),
    );

    // Step 2: Wait for the accept loop or Ctrl+C listener to finish
    // The unselected future is dropped when a branch is selected.
    tokio::select! {
        // Accept loop completed; its result is ignored
        _ = acceptor.run() => {
            tracing::info!("Task succesfully spawned for incoming Redis client");
        },
        // Ctrl+C received or signal-listener error
        _ = signal::ctrl_c() => {
            tracing::info!("Shutdown signal received from OS");
        }
    };

    // Step 3: Request shutdown and wait for sender cleanup
    // Step 3.1: Notify the subscribed handlers of shutdown
    let _ = broadcast_tx.send(());
    // Step 3.2: Release resources owned outside the connection tasks
    // Drop the listener and the acceptor's channel sender clones.
    drop(acceptor);
    drop(mpsc_tx);
    // Step 3.3: Wait for connection-task senders to be dropped
    // No values are sent, so `recv()` returns `None` when all senders are gone.
    // Awaiting this closure suspends the main future while it is pending.
    // There is no timeout on this wait.
    mpsc_rx.recv().await;
    // Step 3.4: Log the shutdown status and final metrics
    tracing::info!("All tasks are safely shut down.");
    info!(
        active_connections = metrics.active_connections(),
        requests_received = metrics.requests_received(),
        command_responses_written = metrics.command_responses_written(),
        cache_hits = metrics.cache_hits(),
        cache_misses = metrics.cache_misses(),
        command_parse_errors = metrics.command_parse_errors(),
        accept_errors = metrics.accept_errors(),
        "Final server metrics"
    );
    Ok(())
}
