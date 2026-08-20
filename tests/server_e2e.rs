use bytes::BytesMut;
use mini_redis::acceptor::Acceptor;
use mini_redis::database::Database;
use mini_redis::metrics::Metrics;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Duration, sleep};

async fn test_server(
    max_connections: usize,
    timeout_duration: Duration,
) -> Result<(SocketAddr, broadcast::Sender<()>, mpsc::Receiver<()>), Box<dyn Error>> {
    // allocate the central memory state.
    let db = Database::new();

    // allocate the global synchronization channels.
    // The broadcast channel requires a capacity limit.
    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<()>(512);
    // The mpsc channel acts as the shutdown latch (capacity 1 is sufficient)
    let (mpsc_tx, mpsc_rx) = mpsc::channel::<()>(1);

    // bind the TCP socket to the designated port.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // Get the assigned address and port
    let address = listener.local_addr()?;

    // prepare an instance of acceptor
    let mut acceptor = Acceptor::new(
        listener,
        db,
        broadcast_tx.clone(),
        mpsc_tx.clone(),
        max_connections,
        timeout_duration,
        Arc::new(Metrics::new()),
    );

    let _handle = tokio::spawn(async move {
        // boot the server
        let _ = acceptor.run().await;
    });

    Ok((address, broadcast_tx, mpsc_rx))
}

mod tests {

    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_happy_path() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx) = test_server(16, Duration::from_secs(60)).await?;
        // create a buffer for the client to receive data
        let mut buffer = BytesMut::with_capacity(4096);

        // Test 1: Valid set request
        // clear the buffer
        buffer.clear();
        // create a TCP client connecting to the server
        let mut test_client = TcpStream::connect(address).await?;
        // the client sends a Redis command "SET Alpha 137"
        test_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nAlpha\r\n$3\r\n137\r\n")
            .await?;
        // write contents to the buffer
        test_client.read_buf(&mut buffer).await?;
        assert_eq!(buffer, b"+OK\r\n"[..]);

        // Test 2: Valid get request
        // clear the buffer
        buffer.clear();
        // the client sends a Redis command "GET Alpha"
        test_client
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nAlpha\r\n")
            .await?;
        // write contents to the buffer
        test_client.read_buf(&mut buffer).await?;
        assert_eq!(buffer, b"$3\r\n137\r\n"[..]);
        // turn off the client
        drop(test_client);

        // broadcast a signal for graceful shutdown
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx) = test_server(16, Duration::from_secs(60)).await?;
        // create a buffer for the client to receive data
        let mut buffer = BytesMut::with_capacity(4096);

        // Test 1: Shutdown signal comes first
        // clear the buffer
        buffer.clear();
        // create a TCP client connecting to the server
        let mut test_client = TcpStream::connect(address).await?;
        // wait until the acceptor finishes its job of spawning and subcsribing;
        // otherwise, it can't receive the shutdown signal
        sleep(Duration::from_millis(50)).await;
        // broadcast a signal for graceful shutdown first
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;

        // We only need to verify that the server has dropped the socket.
        // Once a graceful shutdown is finished, `bytes_read` will be 0.
        let bytes_read = test_client.read_buf(&mut buffer).await?;
        assert_eq!(bytes_read, 0);
        // turn off the client
        drop(test_client);

        Ok(())
    }

    #[tokio::test]
    async fn test_timeout() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx) =
            test_server(16, Duration::from_millis(10)).await?;
        // create a buffer for the client to receive data
        let mut buffer = BytesMut::with_capacity(4096);

        // Test 1: Sleep until timeout
        // clear the buffer
        buffer.clear();
        // create a TCP client connecting to the server
        let mut test_client = TcpStream::connect(address).await?;
        // Sleep longer than the timeout duration
        sleep(Duration::from_millis(20)).await;
        // the client sends a Redis command "SET Alpha 137"
        test_client
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nAlpha\r\n$3\r\n137\r\n")
            .await?;
        // The connection should be closed by server, hence `bytes_read` is 0.
        let bytes_read = test_client.read_buf(&mut buffer).await?;
        assert_eq!(bytes_read, 0);
        // turn off the client
        drop(test_client);

        // broadcast a signal for graceful shutdown
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_protocol_resilience() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx) = test_server(16, Duration::from_secs(60)).await?;
        // create a buffer for the client to receive data
        let mut buffer = BytesMut::with_capacity(4096);

        // Test 1: Invalid Redis frame
        // create a TCP client connecting to the server
        let mut test_client_1 = TcpStream::connect(address).await?;
        // the client sends an invalid Redis frame
        test_client_1.write_all(b"Invalid Redis message").await?;
        // write contents to the buffer
        test_client_1.read_buf(&mut buffer).await?;
        assert_eq!(buffer, b"-Wrong message: Invalid first byte\r\n"[..]);
        // turn off the client
        drop(test_client_1);

        // Test 2: A valid Redis frame, but invalid as a command
        // clear the buffer
        buffer.clear();
        // create a TCP client connecting to the server
        let mut test_client_2 = TcpStream::connect(address).await?;
        // the client sends a valid Redis frame but not supported by the server
        test_client_2
            .write_all(b"*2\r\n$4\r\nDROP\r\n$5\r\nTABLE\r\n")
            .await?;
        // write contents to the buffer
        test_client_2.read_buf(&mut buffer).await?;
        assert_eq!(buffer, b"-Wrong message: not a valid command\r\n"[..]);
        // turn off the client
        drop(test_client_2);

        // broadcast a signal for graceful shutdown
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrency() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx) = test_server(2, Duration::from_secs(60)).await?;

        // The first client
        // create a buffer for the client to receive data
        let mut buffer_1 = BytesMut::with_capacity(4096);
        // create a TCP client connecting to the server
        let mut test_client_1 = TcpStream::connect(address).await?;
        // the client sends a Redis command "SET One 1"
        test_client_1
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nOne\r\n$1\r\n1\r\n")
            .await?;
        // write contents to the buffer
        test_client_1.read_buf(&mut buffer_1).await?;
        assert_eq!(buffer_1, b"+OK\r\n"[..]);

        // The second client
        // create a buffer for the client to receive data
        let mut buffer_2 = BytesMut::with_capacity(4096);
        // create a TCP client connecting to the server
        let mut test_client_2 = TcpStream::connect(address).await?;
        // the client sends a Redis command "SET Two 2"
        test_client_2
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nTwo\r\n$1\r\n2\r\n")
            .await?;
        // write contents to the buffer
        test_client_2.read_buf(&mut buffer_2).await?;
        assert_eq!(buffer_2, b"+OK\r\n"[..]);

        // The third client
        // create a buffer for the client to receive data
        let mut buffer_3 = BytesMut::with_capacity(4096);
        // create a TCP client connecting to the server, which is still allowed
        let mut test_client_3 = TcpStream::connect(address).await?;
        // the client sends a Redis command "SET Three 3"
        test_client_3
            .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nThree\r\n$1\r\n3\r\n")
            .await?;
        // now `test_client_3` is put in the wait list
        let blocked_read = tokio::time::timeout(
            Duration::from_millis(50),
            test_client_3.read_buf(&mut buffer_3),
        )
        .await;
        assert!(blocked_read.is_err());

        // turn off `test_client_1`
        drop(test_client_1);
        // now `test_client_3` is immediately connected to the server
        test_client_3.read_buf(&mut buffer_3).await?;
        assert_eq!(buffer_3, b"+OK\r\n"[..]);

        buffer_3.clear();
        test_client_3
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nThree\r\n")
            .await?;
        test_client_3.read_buf(&mut buffer_3).await?;
        // just double-check the `SET` command works as expected
        assert_eq!(buffer_3, b"$1\r\n3\r\n"[..]);

        // broadcast a signal for graceful shutdown
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;
        Ok(())
    }
}
