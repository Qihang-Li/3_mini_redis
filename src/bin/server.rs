use mini_redis::acceptor::Acceptor;
use mini_redis::database::Database;
use std::error::Error;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Step 0: transplant the observability pipeline for debugging.
    // 0.1 construct a subscriber that prints formatted traces to standard output.
    let subscriber = FmtSubscriber::builder()
        // Defines the max log level to record (TRACE, DEBUG, INFO, WARN, ERROR)
        .with_max_level(Level::INFO)
        // Completes the builder and returns the subscriber
        .finish();

    // 0.2 set the subscriber as the global default for this application.
    tracing::subscriber::set_global_default(subscriber)
        // Fail information
        .expect("Failed to set tracing subscriber");

    // 0.3 emit a test log to verify initialization.
    info!("Mini-Redis Server Daemon initializing...");

    // Step 1: initialize with resource allocation.
    // 1.1 allocate the central memory state.
    let db = Database::new();

    // 1.2 allocate the global synchronization channels.
    // The broadcast channel requires a capacity limit.
    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<()>(512);
    // The mpsc channel acts as the shutdown latch (capacity 1 is sufficient)
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<()>(1);

    // 1.3 bind the TCP socket to the designated port.
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    info!("Server listening on port 6379");

    // 1.4 prepare an instance of acceptor
    let mut acceptor = Acceptor::new(
        listener,
        db,
        broadcast_tx.clone(),
        mpsc_tx.clone(),
        512,
        Duration::from_secs(600),
    );

    // Step 2: Uses `tokio::select!` to race the main thread and shutdown signal
    tokio::select! {
        // Below is the main business logic
        _ = acceptor.run() => {
            tracing::info!("Task succesfully spawned for incoming Redis client");
        },
        _ = signal::ctrl_c() => {
            tracing::info!("Shutdown signal received from OS");
        }
    };

    // Step 3: clean everything after shutdown signal before killing main thread.
    // 3.1 broadcast a signal to every spawned task (as shutdown).
    let _ = broadcast_tx.send(());
    // 3.2 suspend the main thread until the channel closes,
    // which only occurs when the internal Sender count reaches 0.
    drop(mpsc_tx);
    mpsc_rx.recv().await;
    // 3.3 close the main thread.
    tracing::info!("All tasks are safely shut down.");
    Ok(())
}
