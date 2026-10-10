use crate::config;
use crate::frame::Frame;
use bytes::{Buf, BufMut, BytesMut};
use std::error::Error;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    // The wrapped TCP stream buffers outgoing writes.
    stream: BufWriter<TcpStream>,
    // The read buffer retains bytes not yet consumed as complete frames.
    buffer: BytesMut,
}

impl Connection {
    /// Creates a connection with buffered output and an empty read buffer.
    ///
    /// The read buffer can grow beyond its initial capacity.
    /// `read_frame` separately limits how many input bytes it buffers.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY),
        }
    }

    /// Reads and decodes the next frame from the connection.
    ///
    /// Returns `Ok(Some(frame))` for one complete frame, retaining any later
    /// buffered bytes for subsequent calls. Returns `Ok(None)` on EOF when
    /// no input remains buffered.
    ///
    /// # Errors
    /// Returns an error for invalid input, exceeded parsing or buffer limits,
    /// an I/O failure, or EOF while an incomplete frame remains buffered.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error + Send + Sync>> {
        loop {
            // Step 1: Try to parse a frame from the buffered bytes
            let parse_result = self.parse_frame()?;
            if let Some(frame) = parse_result {
                // Return one frame, leaving any later bytes buffered.
                return Ok(Some(frame));
            }

            // Step 2: Reject incomplete input that exhausts the byte budget
            if self.buffer.len() >= config::MAX_BUFFERED_BYTES {
                return Err("Frame cannot fit within the buffer limit.".into());
            }

            // Step 3: Limit the next read to the remaining byte budget
            let bytes_left = config::MAX_BUFFERED_BYTES - self.buffer.len();
            let mut destination = (&mut self.buffer).limit(bytes_left);

            // Step 4: Read more bytes and retry parsing
            let bytes_read = self.stream.read_buf(&mut destination).await?;
            if bytes_read == 0 {
                // EOF with an empty buffer
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                // EOF with an incomplete frame
                return Err("Network Error! Failed to fetch network data.".into());
            }
            // Retry parsing with the newly buffered bytes.
        }
    }

    /// Parses one frame from the beginning of the connection's read buffer.
    ///
    /// Returns `Ok(Some(frame))` and consumes that frame's bytes on success.
    /// Returns `Ok(None)` without consuming bytes if the frame is incomplete.
    ///
    /// # Errors
    /// Returns an error for invalid input or an exceeded implementation limit.
    /// The buffer is left unchanged on error.
    fn parse_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error + Send + Sync>> {
        // Step 1: Create a cursor over the buffered bytes
        let mut cursor = Cursor::new(&self.buffer[..]);

        // Step 2: Parse one frame with `Frame::parse`
        match Frame::parse(&mut cursor) {
            // Complete frame
            Ok(frame) => {
                // Consume only this frame's bytes, preserving later input.
                self.buffer
                    .advance(usize::try_from(cursor.position()).unwrap());
                Ok(Some(frame))
            }
            // Incomplete frame
            Err(crate::frame::Error::Incomplete) => Ok(None),
            // Invalid input or exceeded parsing limit
            Err(e) => Err(e.into()),
        }
    }

    /// Serializes a frame through the buffered stream and flushes its output.
    ///
    /// Encoding may write to the TCP stream before the final flush.
    /// Success does not confirm that the peer has received or processed
    /// the frame.
    ///
    /// # Errors
    /// Returns an error if writing or flushing the stream fails.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: Encode the frame through the buffered writer
        self.write_data(frame).await?;

        // Step 2: Flush any output still buffered to the TCP stream
        self.stream.flush().await?;

        Ok(())
    }

    /// Encodes one frame through the buffered stream without a final flush.
    ///
    /// # Errors
    /// Returns an error if writing to the underlying TCP stream fails.
    async fn write_data(&mut self, frame: &Frame) -> Result<(), Box<dyn Error + Send + Sync>> {
        match frame {
            Frame::Simple(string) => {
                // Step 1: Write the frame marker
                self.stream.write_u8(b'+').await?;
                // Step 2: Write the payload
                self.stream.write_all(string.as_bytes()).await?;
                // Step 3: Write the CRLF terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(string) => {
                // Step 1: Write the frame marker
                self.stream.write_u8(b'-').await?;
                // Step 2: Write the payload
                self.stream.write_all(string.as_bytes()).await?;
                // Step 3: Write the CRLF terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(num) => {
                // Step 1: Write the frame marker
                self.stream.write_u8(b':').await?;
                // Step 2: Write the payload
                self.stream.write_all(num.to_string().as_bytes()).await?;
                // Step 3: Write the CRLF terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Bulk(bytes) => {
                // Step 1: Write the frame marker
                self.stream.write_u8(b'$').await?;
                // Step 2: Write the payload length in bytes
                self.stream
                    .write_all(bytes.len().to_string().as_bytes())
                    .await?;
                // Step 3: Write the CRLF terminator
                self.stream.write_all(b"\r\n").await?;
                // Step 4: Write the payload
                self.stream.write_all(bytes).await?;
                // Step 5: Write the payload's CRLF terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(vec) => {
                // Step 1: Write the frame marker
                self.stream.write_u8(b'*').await?;
                // Step 2: Write the element count
                self.stream
                    .write_all(vec.len().to_string().as_bytes())
                    .await?;
                // Step 3: Write the CRLF terminator
                self.stream.write_all(b"\r\n").await?;
                // Step 4: Encode each child frame
                for frame in vec {
                    // Box the recursive call so the future has a finite size.
                    // Pin the boxed future so it can be polled in place.
                    Box::pin(self.write_data(frame)).await?;
                }
            }
            Frame::Null => {
                // Encode `Null` using the bulk-null representation.
                self.stream.write_all(b"$-1\r\n").await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_connection_new() -> Result<(), Box<dyn Error>> {
        // Bind a TCP listener to an OS-assigned local port.
        let test_listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let test_addr = test_listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let test_stream = TcpStream::connect(test_addr).await?;

        // Check the initial read-buffer capacity.
        let test_connection = Connection::new(test_stream);
        assert_eq!(test_connection.buffer.capacity(), 4096);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Complete simple-string frame
        // Step 1: Send bytes from the client
        client.write_all(b"+Hello, World!\r\n").await?;
        // Step 2: Read a frame at the server endpoint
        let valid_frame = connection.read_frame().await?;
        // Step 3: Check the result
        assert_eq!(
            valid_frame,
            Some(Frame::Simple("Hello, World!".to_string()))
        );

        // Array frame received across two writes
        // Step 1: Send the first part of the array
        client.write_all(b"*2\r\n$3\r\nfoo\r\n").await?;
        let partial_result = timeout(Duration::from_millis(100), connection.read_frame()).await;
        // Expect the read to time out while the frame is incomplete.
        assert!(partial_result.is_err());
        assert_eq!(&connection.buffer[..], b"*2\r\n$3\r\nfoo\r\n");

        // Step 2: Send the remaining element and read the complete frame
        client.write_all(b"$3\r\nbar\r\n").await?;
        let final_result = timeout(Duration::from_millis(1000), connection.read_frame()).await??;
        // Step 3: Check the result
        assert_eq!(
            final_result,
            Some(Frame::Array(vec![
                Frame::Bulk("foo".as_bytes().into()),
                Frame::Bulk("bar".as_bytes().into())
            ]))
        );

        // EOF after the client endpoint is dropped
        // Step 1: Drop the client endpoint
        drop(client);
        // Step 2: Read a frame at the server endpoint
        let dropped_frame = connection.read_frame().await?;
        // Step 3: Check the result
        assert_eq!(dropped_frame, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_oversize() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Complete RESP frame exceeding the buffer limit by one byte
        // Step 1: Send bytes from the client
        // The encoded frame exceeds the buffer limit by one byte.
        let payload = format!("+{}\r\n", "A".repeat(config::MAX_BUFFERED_BYTES + 1 - 3));
        assert_eq!(payload.len(), config::MAX_BUFFERED_BYTES + 1);
        client.write_all(payload.as_bytes()).await?;
        // Step 2: Read a frame at the server endpoint
        let oversized_frame = connection.read_frame().await;
        // Step 3: Check the result
        assert!(oversized_frame.is_err());
        assert_eq!(
            oversized_frame.unwrap_err().to_string(),
            "Frame cannot fit within the buffer limit."
        );
        assert!(connection.buffer.len() == config::MAX_BUFFERED_BYTES);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_incomplete() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Step 1: Send bytes from the client
        // This unterminated input exceeds the buffer limit by one byte.
        let payload = format!("+{}", "A".repeat(config::MAX_BUFFERED_BYTES));
        assert_eq!(payload.len(), config::MAX_BUFFERED_BYTES + 1);
        client.write_all(payload.as_bytes()).await?;
        // Step 2: Read a frame at the server endpoint
        let oversized_frame = timeout(Duration::from_millis(1000), connection.read_frame()).await?;

        // Step 3: Check the result
        assert!(oversized_frame.is_err());
        assert_eq!(
            oversized_frame.unwrap_err().to_string(),
            "Frame cannot fit within the buffer limit."
        );
        assert_eq!(connection.buffer.len(), config::MAX_BUFFERED_BYTES);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_maxsize() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Step 1: Send bytes from the client
        // The encoded frame length equals `config::MAX_BUFFERED_BYTES`.
        let payload = format!("+{}\r\n", "A".repeat(config::MAX_BUFFERED_BYTES - 3));
        assert_eq!(payload.len(), config::MAX_BUFFERED_BYTES);
        client
            .write_all(format!("{payload}:0\r\n").as_bytes())
            .await?;
        // Step 2: Read a frame at the server endpoint
        let maxsized_frame = connection.read_frame().await?;
        // Step 3: Check the result
        assert_eq!(
            maxsized_frame,
            Some(Frame::Simple("A".repeat(config::MAX_BUFFERED_BYTES - 3)))
        );
        let next_frame = connection.read_frame().await?;
        assert_eq!(next_frame, Some(Frame::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_maxsize_reversed()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Step 1: Send bytes from the client
        // The encoded frame length equals `config::MAX_BUFFERED_BYTES`.
        let payload = format!("+{}\r\n", "A".repeat(config::MAX_BUFFERED_BYTES - 3));
        assert_eq!(payload.len(), config::MAX_BUFFERED_BYTES);
        client
            .write_all(format!(":0\r\n{payload}").as_bytes())
            .await?;
        // Step 2: Read a frame at the server endpoint
        let front_frame = connection.read_frame().await?;
        assert_eq!(front_frame, Some(Frame::Integer(0)));
        let maxsized_frame = connection.read_frame().await?;
        // Step 3: Check the result
        assert_eq!(
            maxsized_frame,
            Some(Frame::Simple("A".repeat(config::MAX_BUFFERED_BYTES - 3)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_write_frame() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Set up a connected pair of TCP streams.
        // Bind a TCP listener to an OS-assigned local port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // Read the listener's assigned address.
        let listener_addr = listener.local_addr().unwrap();
        // Connect a client endpoint to the listener.
        let mut client = TcpStream::connect(listener_addr).await?;
        // Accept the server endpoint of the connection.
        let (server, _) = listener.accept().await?;
        // Wrap the server endpoint in a `Connection`.
        let mut connection = Connection::new(server);

        // Simple-string frame
        // Step 1: Write the frame from the server endpoint
        connection
            .write_frame(&Frame::Simple("Hello, World!".to_string()))
            .await?;
        // Step 2: Define the expected RESP bytes
        let expected_1 = b"+Hello, World!\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_1 = vec![0u8; expected_1.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_1),
        )
        .await??;
        // Step 4: Check the received bytes
        assert_eq!(buffer_1.as_slice(), expected_1);

        // Error frame
        // Step 1: Write the frame from the server endpoint
        connection
            .write_frame(&Frame::Error("Error 404 Not Found".to_string()))
            .await?;
        // Step 2: Define the expected RESP bytes
        let expected_2 = b"-Error 404 Not Found\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_2 = vec![0u8; expected_2.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_2),
        )
        .await??;
        // Step 4: Check the received bytes
        assert_eq!(buffer_2.as_slice(), expected_2);

        // Integer frame
        // Step 1: Write the frame from the server endpoint
        connection.write_frame(&Frame::Integer(42i64)).await?;
        // Step 2: Define the expected RESP bytes
        let expected_3 = b":42\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_3 = vec![0u8; expected_3.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_3),
        )
        .await??;
        // Step 4: Check the received bytes
        assert_eq!(buffer_3.as_slice(), expected_3);

        // Bulk-string frame
        // Step 1: Write the frame from the server endpoint
        connection
            .write_frame(&Frame::Bulk("foobar".as_bytes().into()))
            .await?;
        // Step 2: Define the expected RESP bytes
        let expected_4 = b"$6\r\nfoobar\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_4 = vec![0u8; expected_4.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_4),
        )
        .await??;
        // Step 4: Check the received bytes
        assert_eq!(buffer_4.as_slice(), expected_4);

        // Nested-array frame
        // Step 1: Write the frame from the server endpoint
        connection
            .write_frame(&Frame::Array(vec![
                Frame::Array(vec![
                    Frame::Integer(1i64),
                    Frame::Integer(2i64),
                    Frame::Integer(3i64),
                ]),
                Frame::Array(vec![
                    Frame::Simple("Foo".to_string()),
                    Frame::Error("Bar".to_string()),
                ]),
            ]))
            .await?;
        // Step 2: Define the expected RESP bytes
        let expected_5 = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_5 = vec![0u8; expected_5.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_5),
        )
        .await??;
        // Step 4: Check the received bytes
        assert_eq!(buffer_5.as_slice(), expected_5);

        // Null frame
        // Step 1: Write the frame from the server endpoint
        connection.write_frame(&Frame::Null).await?;
        // Step 2: Define the expected RESP bytes
        let expected_bulk_null = b"$-1\r\n";
        let expected_array_null = b"*-1\r\n";
        // Allocate a receive buffer matching the expected byte count.
        let mut buffer_6 = vec![0u8; expected_bulk_null.len()];
        // Step 3: Read the encoded bytes at the client with a timeout
        timeout(
            Duration::from_millis(500),
            // Read exactly the expected number of bytes.
            client.read_exact(&mut buffer_6),
        )
        .await??;
        // Step 4: Check the received bytes
        assert!(
            buffer_6.as_slice() == expected_bulk_null || buffer_6.as_slice() == expected_array_null
        );

        Ok(())
    }
}
