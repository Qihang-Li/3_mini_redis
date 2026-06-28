use super::frame::Frame;
use bytes::Buf;
use bytes::BytesMut;
use std::error::Error;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
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
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Extracts a standardized frame from a given connection.
    ///
    /// # Errors
    /// Returns an error if the network drops the connection abruptly while data
    /// is in the buffer, or if the underlying TCP stream encounters an I/O failure.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        // Input: a reference to Connection, allowing us to modify its buffer.
        // Output: either Ok(Some(Frame)), Ok(None), or Error of a certain kind.

        loop {
            // Step 1: try to read the current buffer and form a frame object.
            if let Some(frame) = self.parse_frame()? {
                // This indicates a successful read. Quit the loop.
                return Ok(Some(frame));
            }

            // Step 2: we are here means the buffer does not contain a full frame.
            // try to read from network and re-run Step 1
            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    // This is a successful disconnect. Quit the loop.
                    return Ok(None);
                }
                // Otherwise, it is an unsuccessful disconnect. Quit the loop.
                return Err("Network Error! Failed to fetch network data.".into());
            }
            // Otherwise, it is an ongoing connection. Re-run the loop.
        }
    }

    /// Extracts a standardized frame from a given buffer.
    ///
    /// # Errors
    /// Returns an incomplete error if the while data in the buffer cannot parse
    /// a full frame, or any other failure.
    fn parse_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        // Input: a reference to Connection, allowing us to modify its buffer.
        // Output: either Ok(Some(Frame)), Ok(None), or Error of a certain kind.

        // Step 1: create a cursor to access the buffer
        let mut cursor = Cursor::new(&self.buffer[..]);

        // Step 2: apply Frame::check() to the cursor
        match Frame::check(&mut cursor) {
            Ok(()) => {
                // Step 3: apply Frame::parse() to the cursor
                // Step 3.1 reset cursor's position to its head
                cursor.set_position(0);
                // Step 3.2 parse the frame using the cursor
                let frame = Frame::parse(&mut cursor)?;
                // Step 3.3 get length of bytes read and update the buffer
                // by removing exact as many bytes read
                self.buffer
                    .advance(usize::try_from(cursor.position()).unwrap());
                // Step 3.4 return the frame
                Ok(Some(frame))
            }
            Err(super::frame::Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write a standardized frame to a given connection.
    ///
    /// # Errors
    /// Returns an error if the network drops the connection abruptly while data
    /// is in the buffer, or if the underlying TCP stream encounters an I/O failure.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), Box<dyn Error>> {
        // Step 1: serialize the entire frame into the RAM buffer recursively
        self.write_data(frame).await?;

        // Step 2: execute a single physical network push
        // flush() is a system call to push all local buffered data to TcpStream,
        // to avoid massive kernel context switches and enhance performance.
        // we used BufWriter<TcpStream> for the same reason.
        self.stream.flush().await?;

        Ok(())
    }

    async fn write_data(&mut self, frame: &Frame) -> Result<(), Box<dyn Error>> {
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

    use super::*;
    //use std::error::Error;
    use tokio::net::TcpListener;

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
    async fn test_connection_read_frame() -> Result<(), Box<dyn Error>> {
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

        // Test 1: Valid full simple string
        // Step 1: write data to the client
        client.write_all(b"+Hello, World!\r\n").await?;
        // Step 2: read data from the connection
        let frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        assert_eq!(frame, Some(Frame::Simple("Hello, World!".to_string())));

        // Test 2: Valid full array, sent in parts
        // Step 1: write data to the client
        client.write_all(b"*2\r\n$3\r\nfoo\r\n").await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        client.write_all(b"$3\r\nbar\r\n").await?;
        /*
        // We may have to write the concurrentcy mannually, in order to
        // force the server to read the first part, hit the Incomplete error,
        // and wake up when Part B arrives.

        let mut background_client = client.try_clone().unwrap();
        tokio::spawn(async move {
            background_client.write_all(b"*2\r\n$3\r\nfoo\r\n").await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            background_client.write_all(b"$3\r\nbar\r\n").await.unwrap();
        });
        */
        // Step 2: read data from the connection
        let frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        assert_eq!(
            frame,
            Some(Frame::Array(vec![
                Frame::Bulk("foo".as_bytes().into()),
                Frame::Bulk("bar".as_bytes().into())
            ]))
        );

        // Test 3: Valid disconnection
        // Step 1: close the connection
        drop(client);
        // Step 2: read data from the connection
        let frame = connection.read_frame().await?;
        // Step 3: compare data to expectation
        assert_eq!(frame, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_connection_write_frame() -> Result<(), Box<dyn Error>> {
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
        let mut buffer = BytesMut::with_capacity(4096);

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
