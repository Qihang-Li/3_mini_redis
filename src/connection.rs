use bytes::BytesMut;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    // has 2 elements: stream and buffer
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    // function new() can instantiate using a stream
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            // assign 4KB of memory for the buffer
            buffer: BytesMut::with_capacity(4096),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::error::Error;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_connection_new() -> Result<(), Box<dyn Error>> {
        // create a TCP server
        let test_listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the server
        let test_addr = test_listener.local_addr().unwrap();
        // create a TCP client connecting to the server
        let test_stream = TcpStream::connect(test_addr).await?;

        // test the new() method
        let test_connection = Connection::new(test_stream);
        assert_eq!(test_connection.buffer.capacity(), 4096);

        Ok(())
    }
}
