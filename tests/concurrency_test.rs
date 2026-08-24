use bytes::Bytes;
use mini_redis::acceptor::Acceptor;
use mini_redis::database::Database;
use mini_redis::metrics::Metrics;
use mini_redis::requester::Requester;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio::time::Duration;

async fn test_server(
    max_connections: usize,
    timeout_duration: Duration,
) -> Result<
    (
        SocketAddr,
        broadcast::Sender<()>,
        mpsc::Receiver<()>,
        Arc<Metrics>,
    ),
    Box<dyn Error>,
> {
    // allocate the central memory state.
    let db = Database::new();

    // allocate the global synchronization channels.
    // The broadcast channel requires a capacity limit.
    let (broadcast_tx, _broadcast_rx) = broadcast::channel::<()>(512);
    // The mpsc channel acts as the shutdown latch (capacity 1 is sufficient)
    let (mpsc_tx, mpsc_rx) = mpsc::channel::<()>(1);

    // bind the TCP socket to the designated port.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // get the assigned address and port
    let address = listener.local_addr()?;
    // initiate a global metric
    let metrics = Arc::new(Metrics::new());

    // prepare an instance of acceptor
    let mut acceptor = Acceptor::new(
        listener,
        db,
        broadcast_tx.clone(),
        mpsc_tx.clone(),
        max_connections,
        timeout_duration,
        metrics.clone(),
    );

    let _handle = tokio::spawn(async move {
        // boot the server
        let _ = acceptor.run().await;
    });

    Ok((address, broadcast_tx, mpsc_rx, metrics))
}

mod tests {

    use std::{assert_eq, time::Duration};

    use super::*;

    #[tokio::test]
    async fn test_high_concurrency_load() -> Result<(), Box<dyn Error>> {
        // create a server using `test_server()`
        let (address, broadcast_tx, mut mpsc_rx, metrics) =
            test_server(256, Duration::from_secs(60)).await?;
        // create a concurrency barrier
        let mut set = JoinSet::new();

        // loop 100 times
        for i in 1..=100 {
            // spawn the task
            set.spawn(async move {
                // create a TCP client connecting to the server
                let mut requester = Requester::connect(address, Duration::from_millis(10))
                    .await
                    .unwrap();

                // set up the payloads
                let key = format!("key_{i}");
                let value = Bytes::from(format!("val_{i}"));

                // execute the `SET` and `GET` commands
                requester.set(&key, value.clone()).await.unwrap();
                let result = requester.get(&key).await.unwrap().unwrap();

                assert_eq!(result, value);

                // turn off the client
                drop(requester);
            });
        }

        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        // broadcast a signal for graceful shutdown
        let _ = broadcast_tx.send(());
        mpsc_rx.recv().await;

        assert_eq!(metrics.total_requests(), 200);
        assert_eq!(metrics.active_connections(), 0);

        Ok(())
    }
}
