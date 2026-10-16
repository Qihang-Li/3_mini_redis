use bytes::Bytes;
use mini_redis::acceptor::Acceptor;
use mini_redis::config;
use mini_redis::database::Database;
use mini_redis::metrics::Metrics;
use mini_redis::requester::Requester;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, timeout};

/// Binds a test listener and spawns the acceptor task.
///
/// Returns the bound address, shutdown sender, completion receiver, shared
/// metrics, and acceptor task handle, in that order.
/// `timeout_duration` configures each handler's frame-read timeout.
///
/// # Errors
///
/// Returns an error if binding the listener or reading its local address fails.
async fn spawn_test_server(
    max_connections: usize,
    timeout_duration: Duration,
) -> Result<
    (
        SocketAddr,
        broadcast::Sender<()>,
        mpsc::Receiver<()>,
        Arc<Metrics>,
        JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    ),
    Box<dyn Error>,
> {
    // Step 1: Create shared state and shutdown channels
    let database = Database::new();

    // Broadcast requests shutdown from subscribed tasks.
    let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);
    // No completion messages are sent; sender lifetimes track cleanup.
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel::<()>(1);

    // Step 2: Bind a local listener on an OS-assigned port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // Obtain the actual address after binding to port 0.
    let server_addr = listener.local_addr()?;
    // Share this server's metrics with the test.
    let metrics = Arc::new(Metrics::new());

    // Step 3: Configure and spawn the acceptor
    let mut acceptor = Acceptor::new(
        listener,
        database,
        shutdown_tx.clone(),
        shutdown_complete_tx.clone(),
        max_connections,
        timeout_duration,
        metrics.clone(),
    );

    let acceptor_handle = tokio::spawn(async move {
        // Preserve the accept loop result for the test to inspect.
        acceptor.run().await
    });

    // The local completion sender drops on return.
    // The acceptor keeps its clone.
    Ok((
        server_addr,
        shutdown_tx,
        shutdown_complete_rx,
        metrics,
        acceptor_handle,
    ))
}

mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_high_concurrency_load() -> Result<(), Box<dyn Error>> {
        // Keep connection setup, I/O, and assertions within one test scenario.
        let scenario = async {
            // Step 1: Start the test server and prepare client-task tracking
            // Allow 256 handlers, each with a 60-second frame-read timeout.
            let (server_addr, shutdown_tx, mut shutdown_complete_rx, metrics, mut acceptor_handle) =
                spawn_test_server(256, Duration::from_secs(60)).await?;
            // Track client tasks so their completion and failures can be checked.
            let mut client_tasks = JoinSet::new();

            // Step 2: Spawn 100 client tasks, each with its own SET/GET pair
            for client_id in 1..=100 {
                client_tasks.spawn(async move {
                    // Apply 500 ms separately to connecting, writing, and reading.
                    let mut requester = Requester::connect(server_addr, Duration::from_millis(500))
                        .await
                        .unwrap();

                    // Use a unique key so clients do not overwrite each other.
                    let key = format!("key_{client_id}");
                    let value = Bytes::from(format!("val_{client_id}"));

                    // Keep `value` available for comparison by passing a clone.
                    requester.set(&key, value.clone()).await.unwrap();
                    let returned_value = requester.get(&key).await.unwrap().unwrap();

                    // Verify that GET returns the value written by this client.
                    assert_eq!(returned_value, value);

                    // Release the client connection after verification.
                    // Server-side cleanup may finish later.
                    drop(requester);
                });
            }

            // Step 3: Collect every client task result
            // Tasks are returned in completion order.
            while let Some(task_result) = client_tasks.join_next().await {
                // Fail the test if a client task panicked or was cancelled.
                task_result.unwrap();
            }

            // Step 4: Request shutdown and wait for completion-channel closure
            let _ = shutdown_tx.send(());
            // Give this channel wait its own one-second timeout.
            match timeout(Duration::from_secs(1), shutdown_complete_rx.recv()).await {
                // All completion senders have been dropped
                Ok(None) => {}
                // Unexpected message on a channel used only for sender tracking
                Ok(Some(())) => {
                    // Request cancellation before reporting the channel misuse.
                    acceptor_handle.abort();
                    return Err("Unexpected message on shutdown completion channel".into());
                }
                // This wait timed out
                Err(elapsed) => {
                    // Request cancellation before reporting the timeout.
                    acceptor_handle.abort();
                    return Err(format!(
                        "Timed out waiting for shutdown completion channel closure: {elapsed}"
                    )
                    .into());
                }
            }

            // Step 5: Join the acceptor task with a separate one-second timeout
            match timeout(Duration::from_secs(1), &mut acceptor_handle).await {
                // Wait completed, task joined, and acceptor returned success
                Ok(Ok(Ok(()))) => {}
                // Acceptor returned an error
                Ok(Ok(Err(acceptor_error))) => {
                    let error: Box<dyn Error> = acceptor_error;
                    return Err(error);
                }
                // Acceptor task panicked or was cancelled
                Ok(Err(join_error)) => return Err(join_error.into()),
                // This wait timed out
                Err(elapsed) => {
                    // Request cancellation before reporting the timeout.
                    acceptor_handle.abort();
                    return Err(format!(
                        "Timed out waiting for the acceptor task to finish: {elapsed}"
                    )
                    .into());
                }
            }

            // Step 6: Check the final request counts and connection cleanup
            // Each of 100 clients completed one SET and one GET.
            assert_eq!(metrics.command_responses_written(), 200);
            assert_eq!(metrics.requests_received(), 200);
            assert_eq!(metrics.active_connections(), 0);

            Ok::<(), Box<dyn Error>>(())
        };

        // Give the complete scenario one shared five-second waiting budget.
        // Propagate either timeout expiry or an error returned by the scenario.
        timeout(Duration::from_secs(5), scenario).await??;
        Ok(())
    }
}
