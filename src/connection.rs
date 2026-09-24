use crate::config;
use crate::frame::Frame;
use bytes::{Buf, BufMut, BytesMut};
use std::error::Error;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    // Connection struct has 2 elements: stream and buffer
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        // Instantiates a new Connection
        // Input: stream as a TcpStream
        // Output: a Connection object
        // Output: stream as a BufWriter of TcpStream
        // Output: buffer as a chuck of memory being 4 KB
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY),
        }
    }

    /// Extracts a standardized frame from a given connection.
    ///
    /// # Errors
    /// Returns an error if the network drops the connection abruptly while data
    /// is in the buffer, or if the underlying TCP stream encounters an I/O failure.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error + Send + Sync>> {
        // Input: a reference to Connection, allowing us to modify its buffer.
        // Output: either Ok(Some(Frame)), Ok(None), or Error of a certain kind.

        loop {
            // Step 1: try to read the current buffer and form a frame object.
            let parse_result = self.parse_frame()?;
            if let Some(frame) = parse_result {
                // This indicates a successful read. Quit the loop.
                return Ok(Some(frame));
            }

            // Step 2: reaching this point means `parse_frame()` returned `Ok(None)`
            // check if there is any remaining byte budget
            if self.buffer.len() >= config::MAX_BUFFERED_BYTES {
                return Err("Frame cannot fit within the buffer limit.".into());
            }

            // Step 3: set a limit on how many more bytes the buffer can read from network
            let bytes_left = config::MAX_BUFFERED_BYTES - self.buffer.len();
            let mut destination = (&mut self.buffer).limit(bytes_left);

            // Step 4: reaching this point means the buffer does not contain a full frame.
            // try to read from network and re-run Step 1
            let bytes_read = self.stream.read_buf(&mut destination).await?;
            if bytes_read == 0 {
                // 4.(i) a successful disconnect from the client. Quit the loop.
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                // 4.(ii) an unsuccessful disconnect. Quit the loop.
                return Err("Network Error! Failed to fetch network data.".into());
            }
            // Otherwise, it is an ongoing connection. Re-run the loop.
        }
    }

    /// Parses one frame from the beginning of the connection's read buffer.
    ///
    /// Returns `Ok(Some(frame))` and consumes that frame's bytes on success.
    /// Returns `Ok(None)` without consuming buffered bytes if more input is needed.
    ///
    /// # Errors
    /// Returns an error for invalid input or an exceeded implementation limit.
    /// The buffer is left unchanged on error.
    fn parse_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error + Send + Sync>> {
        // Input: a reference to Connection, allowing us to modify its buffer.
        // Output: either Ok(Some(Frame)), Ok(None), or Error of a certain kind.

        // Step 1: create a cursor to access the buffer
        let mut cursor = Cursor::new(&self.buffer[..]);

        // Step 2: apply Frame::parse() to the cursor
        match Frame::parse(&mut cursor) {
            // 2.(i) a valid `Frame`
            Ok(frame) => {
                // get length of bytes read and update the buffer
                // by removing exact as many bytes read
                self.buffer
                    .advance(usize::try_from(cursor.position()).unwrap());
                Ok(Some(frame))
            }
            // 2.(ii) an incomplete `Frame`
            Err(crate::frame::Error::Incomplete) => Ok(None),
            // 2.(iii) an invalid `Frame`
            Err(e) => Err(e.into()),
        }
    }

    /// Writes a standardized frame to a given connection.
    ///
    /// # Errors
    /// Returns an error if the network drops the connection abruptly while data
    /// is in the buffer, or if the underlying TCP stream encounters an I/O failure.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: serialize the entire frame into the RAM buffer recursively
        self.write_data(frame).await?;

        // Step 2: execute a single physical network push
        // flush() is a system call to push all local buffered data to TcpStream,
        // to avoid massive kernel context switches and enhance performance.
        // we used BufWriter<TcpStream> for the same reason.
        self.stream.flush().await?;

        Ok(())
    }

    async fn write_data(&mut self, frame: &Frame) -> Result<(), Box<dyn Error + Send + Sync>> {
        match frame {
            Frame::Simple(string) => {
                // 1. write the identifying byte
                self.stream.write_u8(b'+').await?;
                // 2. write the payload
                self.stream.write_all(string.as_bytes()).await?;
                // 3. write the terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(string) => {
                // 1. write the identifying byte
                self.stream.write_u8(b'-').await?;
                // 2. write the payload
                self.stream.write_all(string.as_bytes()).await?;
                // 3. write the terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(num) => {
                // 1. write the identifying byte
                self.stream.write_u8(b':').await?;
                // 2. write the payload
                self.stream.write_all(num.to_string().as_bytes()).await?;
                // 3. write the terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Bulk(bytes) => {
                // 1. write the identifying byte
                self.stream.write_u8(b'$').await?;
                // 2. write the length
                self.stream
                    .write_all(bytes.len().to_string().as_bytes())
                    .await?;
                // 3. write the terminator
                self.stream.write_all(b"\r\n").await?;
                // 4. write the payload
                self.stream.write_all(bytes).await?;
                // 5. write the terminator
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(vec) => {
                // 1. write the identifying byte
                self.stream.write_u8(b'*').await?;
                // 2. write the size
                self.stream
                    .write_all(vec.len().to_string().as_bytes())
                    .await?;
                // 3. write the terminator
                self.stream.write_all(b"\r\n").await?;
                // 4. write all the sub-frames
                for frame in vec {
                    // here we use the Box::pin() method to handle async,
                    // guaranteeing its physical RAM address will never change
                    Box::pin(self.write_data(frame)).await?;
                }
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::assert_eq;

    use super::*;
    use bytes::Bytes;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_connection_new() -> Result<(), Box<dyn Error>> {
        // create a TCP listener
        let test_listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let test_addr = test_listener.local_addr().unwrap();
        // create a TCP client connecting to the listener
        let test_stream = TcpStream::connect(test_addr).await?;

        // test the new() method
        let test_connection = Connection::new(test_stream);
        assert_eq!(test_connection.buffer.capacity(), 4096);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a router or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);

        // Test 1: Valid full simple string
        // Step 1: write data to the client
        client.write_all(b"+Hello, World!\r\n").await?;
        // Step 2: read data from the connection
        let valid_frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        assert_eq!(
            valid_frame,
            Some(Frame::Simple("Hello, World!".to_string()))
        );

        // Test 2: Valid full array, sent in parts
        // Step 1: send the first part only
        client.write_all(b"*2\r\n$3\r\nfoo\r\n").await?;
        let partial_result = timeout(Duration::from_millis(100), connection.read_frame()).await;
        // here `partial_result` should be `Err(Elapsed)`, indicating `read_frame()` is pending
        assert!(partial_result.is_err());
        assert_eq!(&connection.buffer[..], b"*2\r\n$3\r\nfoo\r\n");

        // Step 2: now send the second part
        client.write_all(b"$3\r\nbar\r\n").await?;
        let final_result = timeout(Duration::from_millis(200), connection.read_frame()).await??;
        // Step 3: compare data to expectation
        assert_eq!(
            final_result,
            Some(Frame::Array(vec![
                Frame::Bulk("foo".as_bytes().into()),
                Frame::Bulk("bar".as_bytes().into())
            ]))
        );

        // Test 3: Valid disconnection
        // Step 1: close the connection
        drop(client);
        // Step 2: read data from the connection
        let dropped_frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        assert_eq!(dropped_frame, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_oversize() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a router or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);

        // Test 1: Valid RESP frame exceeding the buffer limit by one byte
        // Step 1: write data to the client
        let size = config::MAX_BUFFERED_BYTES + 1
            - 1
            - usize::try_from(config::MAX_BUFFERED_BYTES.checked_ilog10().unwrap_or(0) + 1)
                .unwrap()
            - 2
            - 2;
        let placeholder = "A".repeat(size);
        client
            // that's exactly 65537 bytes
            .write_all(format!("${size}\r\n{placeholder}\r\n").as_bytes())
            .await?;
        // Step 2: read data from the connection
        let oversized_frame = connection.read_frame().await;
        // Step 3: compare data to expectation
        assert!(oversized_frame.is_err());
        assert_eq!(
            oversized_frame.unwrap_err().to_string(),
            "Frame cannot fit within the buffer limit."
        );
        assert!(connection.buffer.len() <= config::MAX_BUFFERED_BYTES);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_incomplete() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a router or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);

        // Test 1: Valid RESP frame exceeding the buffer limit by one byte
        // Step 1: write data to the client
        let placeholder = "A".repeat(config::MAX_BUFFERED_BYTES);
        client
            // that's exactly 65537 bytes
            .write_all(format!("+{placeholder}").as_bytes())
            .await?;
        // Step 2: read data from the connection
        let oversized_frame = timeout(Duration::from_millis(100), connection.read_frame()).await?;
        // Step 3: compare data to expectation
        assert!(oversized_frame.is_err());
        assert_eq!(
            oversized_frame.unwrap_err().to_string(),
            "Frame cannot fit within the buffer limit."
        );
        assert!(connection.buffer.len() <= config::MAX_BUFFERED_BYTES);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_maxsize() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a router or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);

        // Test 2: Valid frame of exactly MAX_BUFFERED_BYTES bytes, followed by another frame
        // Step 1: write data to the client
        let size = config::MAX_BUFFERED_BYTES
            - 1
            - usize::try_from(config::MAX_BUFFERED_BYTES.checked_ilog10().unwrap_or(0) + 1)
                .unwrap()
            - 2
            - 2;
        let placeholder = "A".repeat(size);
        client
            // The bulk frame is exactly 65,536 bytes
            .write_all(format!("${size}\r\n{placeholder}\r\n:0\r\n").as_bytes())
            .await?;
        // Step 2: read data from the connection
        let maxsized_frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        //assert!(maxsized_frame.is_ok());
        assert_eq!(maxsized_frame, Some(Frame::Bulk(Bytes::from(placeholder))));
        assert!(connection.buffer.len() <= config::MAX_BUFFERED_BYTES);
        let next_frame = connection.read_frame().await?;
        assert_eq!(next_frame, Some(Frame::Integer(0)));

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_read_frame_maxsize_reversed()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a router or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);

        // Test 3: Valid frame, followed by a frame of exactly MAX_BUFFERED_BYTES bytes
        // Step 1: write data to the client
        let size = config::MAX_BUFFERED_BYTES
            - 1
            - usize::try_from(config::MAX_BUFFERED_BYTES.checked_ilog10().unwrap_or(0) + 1)
                .unwrap()
            - 2
            - 2;
        let placeholder = "A".repeat(size);
        client
            // The bulk frame is exactly 65,536 bytes
            .write_all(format!(":0\r\n${size}\r\n{placeholder}\r\n").as_bytes())
            .await?;
        // Step 2: read data from the connection
        let front_frame = connection.read_frame().await?;
        assert_eq!(front_frame, Some(Frame::Integer(0)));
        let maxsized_frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        //assert!(maxsized_frame.is_ok());
        assert_eq!(maxsized_frame, Some(Frame::Bulk(Bytes::from(placeholder))));
        assert!(connection.buffer.len() <= config::MAX_BUFFERED_BYTES);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_write_frame() -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 0: environment setup
        // create a TCP listener (a rounter or switch)
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        // get address of the listener
        let listener_addr = listener.local_addr().unwrap();
        // create a TCP client (a gate, either entrance or exit) connecting to the listener
        let mut client = TcpStream::connect(listener_addr).await?;
        // create a TCP server  (a gate, either entrance or exit) for the client
        let (server, _) = listener.accept().await?;
        // create a connection from the server
        let mut connection = Connection::new(server);
        // create a buffer
        let mut buffer = BytesMut::with_capacity(config::INITIAL_READ_BUFFER_CAPACITY);

        // Test 1: Valid simple frame
        // Step 1: write data to the connection
        connection
            .write_frame(&Frame::Simple("Hello, World!".to_string()))
            .await?;
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert_eq!(&buffer[..], b"+Hello, World!\r\n");

        // Test 2: Valid error frame
        // Step 1: write data to the connection
        connection
            .write_frame(&Frame::Error("Error 404 Not Found".to_string()))
            .await?;
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert_eq!(&buffer[..], b"-Error 404 Not Found\r\n");

        // Test 3: Valid integer frame
        // Step 1: write data to the connection
        connection.write_frame(&Frame::Integer(42i64)).await?;
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert_eq!(&buffer[..], b":42\r\n");

        // Test 4: Valid bulk string frame
        // Step 1: write data to the connection
        connection
            .write_frame(&Frame::Bulk("foobar".as_bytes().into()))
            .await?;
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert_eq!(&buffer[..], b"$6\r\nfoobar\r\n");

        // Test 5: Valid error frame
        // Step 1: write data to the connection
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
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert_eq!(
            &buffer[..],
            b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"
        );

        // Test 6: Valid error frame
        // Step 1: write data to the connection
        connection.write_frame(&Frame::Null).await?;
        // Step 2: reset buffer and read data from the client
        buffer.clear();
        client.read_buf(&mut buffer).await?;
        // Step 3: compare data to expectation
        assert!(&buffer[..] == b"$-1\r\n" || &buffer[..] == b"*-1\r\n");

        Ok(())
    }
}
