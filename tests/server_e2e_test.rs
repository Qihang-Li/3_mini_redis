use bytes::BytesMut;
use mini_redis::acceptor::Acceptor;
use mini_redis::config;
use mini_redis::database::Database;
use mini_redis::metrics::Metrics;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep, timeout};

/// Binds a test listener and spawns the acceptor task.
///
/// Returns the bound address, shutdown sender, completion receiver, and
/// acceptor task handle, in that order.
/// `timeout_duration` configures each handler's frame-read timeout.
///
/// # Errors
///
/// Returns an error if binding the listener or reading its local address fails.
async fn test_server(
    max_connections: usize,
    timeout_duration: Duration,
) -> Result<
    (
        SocketAddr,
        broadcast::Sender<()>,
        mpsc::Receiver<()>,
        JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    ),
    Box<dyn Error>,
> {
    // Step 1: Create shared state and shutdown channels
    let db = Database::new();

    // Broadcast requests shutdown from subscribed tasks.
    let (broadcast_tx, _broadcast_rx) =
        broadcast::channel::<()>(config::SHUTDOWN_BROADCAST_CAPACITY);
    // No completion messages are sent; sender lifetimes track cleanup.
    let (mpsc_tx, mpsc_rx) = mpsc::channel::<()>(1);

    // Step 2: Bind a local listener on an OS-assigned port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // Obtain the actual address after binding to port 0.
    let address = listener.local_addr()?;

    // Step 3: Configure and spawn the acceptor
    let mut acceptor = Acceptor::new(
        listener,
        db,
        broadcast_tx.clone(),
        mpsc_tx.clone(),
        max_connections,
        timeout_duration,
        Arc::new(Metrics::new()),
    );

    let handle = tokio::spawn(async move {
        // Preserve the accept loop result for the test to inspect.
        acceptor.run().await
    });

    // The local mpsc sender drops on return; the acceptor keeps its clone.
    Ok((address, broadcast_tx, mpsc_rx, handle))
}

mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_happy_path_and_shutdown() -> Result<(), Box<dyn Error>> {
        // Start a server with a 60-second frame-read timeout.
        let (address, broadcast_tx, mut mpsc_rx, mut server_handle) =
            test_server(16, Duration::from_secs(60)).await?;

        // Valid SET request
        // Connect a client to the test server.
        let mut test_client = TcpStream::connect(address).await?;
        // Send `SET Alpha 137` as a RESP array.
        test_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nAlpha\r\n$3\r\n137\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_1 = b"+OK\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_1 = vec![0u8; expected_1.len()];
        // Read the complete expected response with a 500 ms timeout.
        // TCP may deliver the response across multiple reads.
        timeout(
            Duration::from_millis(500),
            test_client.read_exact(&mut buffer_1),
        )
        .await??;
        assert_eq!(buffer_1.as_slice(), expected_1);

        // Valid GET request
        // Send `GET Alpha` over the same connection.
        test_client
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nAlpha\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_2 = b"$3\r\n137\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_2 = vec![0u8; expected_2.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client.read_exact(&mut buffer_2),
        )
        .await??;
        assert_eq!(buffer_2.as_slice(), expected_2);

        // Shutdown while the client remains connected
        // Request shutdown and allow one second for sender cleanup.
        let _ = broadcast_tx.send(());
        match timeout(Duration::from_secs(1), mpsc_rx.recv()).await {
            // All completion senders have been dropped
            Ok(None) => {}
            // Unexpected completion message
            Ok(Some(())) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err("unexpected message on shutdown completion channel".into());
            }
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for shutdown channel closure: {elapsed}").into(),
                );
            }
        }

        // Allocate a buffer for the client to receive data.
        let mut buffer = BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY);
        // Both command responses were consumed above.
        // The next read should observe EOF from the server.
        let bytes_read = timeout(
            Duration::from_millis(500),
            test_client.read_buf(&mut buffer),
        )
        .await??;
        assert_eq!(bytes_read, 0);
        // Drop the client connection after observing EOF.
        drop(test_client);

        // Join the acceptor task with a separate one-second timeout.
        match timeout(Duration::from_secs(1), &mut server_handle).await {
            // Task joined and acceptor returned success
            Ok(Ok(Ok(()))) => {}
            // Acceptor returned an error
            Ok(Ok(Err(server_error))) => {
                let error: Box<dyn Error> = server_error;
                return Err(error);
            }
            // Acceptor task panicked or was cancelled
            Ok(Err(join_error)) => return Err(join_error.into()),
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for the server task to finish: {elapsed}").into(),
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout() -> Result<(), Box<dyn Error>> {
        // Start a server with a 10 ms frame-read timeout.
        let (address, broadcast_tx, mut mpsc_rx, mut server_handle) =
            test_server(16, Duration::from_millis(10)).await?;
        // Allocate a buffer for the client to receive data.
        let mut buffer = BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY);

        // Idle connection timeout
        // Clear the receive buffer.
        buffer.clear();
        // Connect a client to the test server.
        let mut test_client = TcpStream::connect(address).await?;
        // Leave the client idle longer than the configured read timeout.
        sleep(Duration::from_millis(100)).await;
        // Check for EOF with a separate 500 ms timeout.
        let bytes_read = timeout(
            Duration::from_millis(500),
            test_client.read_buf(&mut buffer),
        )
        .await??;
        assert_eq!(bytes_read, 0);
        // Drop the client connection after observing EOF.
        drop(test_client);

        // Server shutdown
        // Request shutdown and allow one second for sender cleanup.
        let _ = broadcast_tx.send(());
        match timeout(Duration::from_secs(1), mpsc_rx.recv()).await {
            // All completion senders have been dropped
            Ok(None) => {}
            // Unexpected completion message
            Ok(Some(())) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err("unexpected message on shutdown completion channel".into());
            }
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for shutdown channel closure: {elapsed}").into(),
                );
            }
        }

        // Join the acceptor task with a separate one-second timeout.
        match timeout(Duration::from_secs(1), &mut server_handle).await {
            // Task joined and acceptor returned success
            Ok(Ok(Ok(()))) => {}
            // Acceptor returned an error
            Ok(Ok(Err(server_error))) => {
                let error: Box<dyn Error> = server_error;
                return Err(error);
            }
            // Acceptor task panicked or was cancelled
            Ok(Err(join_error)) => return Err(join_error.into()),
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for the server task to finish: {elapsed}").into(),
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_protocol_resilience() -> Result<(), Box<dyn Error>> {
        // Start a server with a 60-second frame-read timeout.
        let (address, broadcast_tx, mut mpsc_rx, mut server_handle) =
            test_server(16, Duration::from_secs(60)).await?;

        // Malformed RESP frame
        // Connect a client to the test server.
        let mut test_client_1 = TcpStream::connect(address).await?;
        // Send bytes with an invalid RESP type prefix.
        test_client_1.write_all(b"Invalid Redis message").await?;
        // Define the expected response bytes.
        let expected_1 = b"-Wrong message: Invalid first byte\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_1 = vec![0u8; expected_1.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_1.read_exact(&mut buffer_1),
        )
        .await??;
        assert_eq!(buffer_1.as_slice(), expected_1);

        // Unsupported command in a valid RESP array
        // Use a separate connection for the unsupported command.
        let mut test_client_2 = TcpStream::connect(address).await?;
        // Send the unsupported command `DROP TABLE` as a valid RESP array.
        test_client_2
            .write_all(b"*2\r\n$4\r\nDROP\r\n$5\r\nTABLE\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_2 = b"-Wrong message: not a valid command\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_2 = vec![0u8; expected_2.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_2.read_exact(&mut buffer_2),
        )
        .await??;
        assert_eq!(buffer_2.as_slice(), expected_2);

        // Server shutdown
        // Request shutdown and allow one second for sender cleanup.
        let _ = broadcast_tx.send(());
        match timeout(Duration::from_secs(1), mpsc_rx.recv()).await {
            // All completion senders have been dropped
            Ok(None) => {}
            // Unexpected completion message
            Ok(Some(())) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err("unexpected message on shutdown completion channel".into());
            }
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for shutdown channel closure: {elapsed}").into(),
                );
            }
        }

        // Join the acceptor task with a separate one-second timeout.
        match timeout(Duration::from_secs(1), &mut server_handle).await {
            // Task joined and acceptor returned success
            Ok(Ok(Ok(()))) => {}
            // Acceptor returned an error
            Ok(Ok(Err(server_error))) => {
                let error: Box<dyn Error> = server_error;
                return Err(error);
            }
            // Acceptor task panicked or was cancelled
            Ok(Err(join_error)) => return Err(join_error.into()),
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for the server task to finish: {elapsed}").into(),
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrency() -> Result<(), Box<dyn Error>> {
        // Allow two handlers, each with a 60-second frame-read timeout.
        let (address, broadcast_tx, mut mpsc_rx, mut server_handle) =
            test_server(2, Duration::from_secs(60)).await?;

        // First client
        // Connect a client to the test server.
        let mut test_client_1 = TcpStream::connect(address).await?;
        // Send `SET One 1` and verify its response.
        test_client_1
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nOne\r\n$1\r\n1\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_1 = b"+OK\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_1 = vec![0u8; expected_1.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_1.read_exact(&mut buffer_1),
        )
        .await??;
        assert_eq!(buffer_1.as_slice(), expected_1);

        // Second client
        // Connect a client to the test server.
        let mut test_client_2 = TcpStream::connect(address).await?;
        // Send `SET Two 2` and verify its response.
        test_client_2
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nTwo\r\n$1\r\n2\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_2 = b"+OK\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_2 = vec![0u8; expected_2.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_2.read_exact(&mut buffer_2),
        )
        .await??;
        assert_eq!(buffer_2.as_slice(), expected_2);

        // Third client waits for connection capacity
        // Keep both existing clients open to occupy the two handler slots.
        // Allocate a buffer for the client to receive data.
        let mut buffer_3 = BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY);
        // Connect a third client to the listening socket.
        let mut test_client_3 = TcpStream::connect(address).await?;
        // Send `SET Three 3` before a handler slot becomes available.
        test_client_3
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nThree\r\n$1\r\n3\r\n")
            .await?;

        // With both slots occupied, the acceptor waits for a permit.
        // Verify that no response arrives during this 100 ms window.
        let blocked_read = tokio::time::timeout(
            Duration::from_millis(100),
            test_client_3.read_buf(&mut buffer_3),
        )
        .await;
        assert!(blocked_read.is_err());

        // Close the first client so its handler can release a permit.
        drop(test_client_1);

        // Third client is served after a permit becomes available
        // Wait for the SET reply after the first handler releases its permit.
        // Define the expected response bytes.
        let expected_3 = b"+OK\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_3 = vec![0u8; expected_3.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_3.read_exact(&mut buffer_3),
        )
        .await??;
        assert_eq!(buffer_3.as_slice(), expected_3);
        // Send `GET Three` over the same connection.
        test_client_3
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nThree\r\n")
            .await?;
        // Define the expected response bytes.
        let expected_4 = b"$1\r\n3\r\n";
        // Allocate space for exactly the expected response.
        let mut buffer_4 = vec![0u8; expected_4.len()];
        // Read the complete expected response with a 500 ms timeout.
        timeout(
            Duration::from_millis(500),
            test_client_3.read_exact(&mut buffer_4),
        )
        .await??;
        assert_eq!(buffer_4.as_slice(), expected_4);

        // Server shutdown
        // Request shutdown and allow one second for sender cleanup.
        let _ = broadcast_tx.send(());
        match timeout(Duration::from_secs(1), mpsc_rx.recv()).await {
            // All completion senders have been dropped
            Ok(None) => {}
            // Unexpected completion message
            Ok(Some(())) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err("unexpected message on shutdown completion channel".into());
            }
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for shutdown channel closure: {elapsed}").into(),
                );
            }
        }

        // Join the acceptor task with a separate one-second timeout.
        match timeout(Duration::from_secs(1), &mut server_handle).await {
            // Task joined and acceptor returned success
            Ok(Ok(Ok(()))) => {}
            // Acceptor returned an error
            Ok(Ok(Err(server_error))) => {
                let error: Box<dyn Error> = server_error;
                return Err(error);
            }
            // Acceptor task panicked or was cancelled
            Ok(Err(join_error)) => return Err(join_error.into()),
            // This wait timed out
            Err(elapsed) => {
                // Request cancellation before reporting the failure.
                server_handle.abort();
                return Err(
                    format!("timed out waiting for the server task to finish: {elapsed}").into(),
                );
            }
        }

        Ok(())
    }
}
