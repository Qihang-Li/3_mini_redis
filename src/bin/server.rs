use mini_redis::database::Database;
use std::error::Error;
use std::unimplemented;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Step 1: Initialization with resource allocation.
    // 1.1: Transplants the observability pipeline.
    // Constructs a subscriber that prints formatted traces to standard output.
    let subscriber = FmtSubscriber::builder()
        // Defines the max log level to record (TRACE, DEBUG, INFO, WARN, ERROR)
        .with_max_level(Level::INFO)
        // Completes the builder and returns the subscriber
        .finish();

    // Sets the subscriber as the global default for this application.
    tracing::subscriber::set_global_default(subscriber)
        // Fail information
        .expect("Failed to set tracing subscriber");

    // Emits a test log to verify initialization.
    info!("Mini-Redis Server Daemon initializing...");

    // 1.2: Allocates the central memory state.
    let db = Database::new();

    // 1.3: Allocates the global synchronization channels.
    // The broadcast channel requires a capacity limit.
    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<()>(512);
    // The mpsc channel acts as the shutdown latch (capacity 1 is sufficient)
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<()>(1);

    // 1.4: Binds the TCP socket to the designated port.
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    info!("Server listening on port 6379");

    // Step 2: Sets an event loop for connection accepting and signal handling
    loop {
        // Uses `tokio::select!` for the race condition
        tokio::select! {
            // We need TcpStream to init `Handler`, as a result of listener.accept(),
            // which returns a Result<(TcpStream, SocketAddr), std::io::Error>.
            Ok((socket, _)) = listener.accept() => {
                // Sets up tools for the spawned task.
                let db_clone = db.clone();
                let broadcast_rx_clone = broadcast_tx.subscribe();
                let mpsc_tx_clone = mpsc_tx.clone();
                let handle = tokio::spawn(async move {
                    // main business to call
                    unimplemented!();
                });
            }
            _ = signal::ctrl_c() => {
                break;
            }
        };
    }

    // Step 3: Cleans everything after shutdown signal before killing main thread.
    // Broadcasts a signal to every spawned task (as shutdown).
    let _ = broadcast_tx.send(());
    // Suspends the main thread until the channel closes,
    // which only occurs when the internal Sender count reaches 0.
    drop(mpsc_tx);
    mpsc_rx.recv().await;
    // Closes the main thread.
    tracing::info!("All tasks are safely shut down.");
    Ok(())
}
