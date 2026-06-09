use super::frame::Frame;
use bytes::BytesMut;
use std::error::Error;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    // has 2 elements: stream and buffer
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        // function new() can instantiate using a stream
        Self {
            stream,
            // assign 4KB of memory for the buffer
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Extracts a standardized frame from a given connection.
    ///
    /// # Errors
    /// Returns an error if the network drops the connection abruptly while data
    /// is in the buffer, or if the underlying TCP stream encounters an I/O failure.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        // Input: a reference to Connection , allowing us to modify its buffer.
        // Output: Option<frame> gives Some(Frame) or None,
        // Output: Box<dyn Error> wraps all possible errors.
        // Output: The final result shall be either Ok(Some(Frame)),
        // Output: Ok(None), or Error of a certain kind.

        loop {
            // Step 1: try to read the current buffer and form a frame object.
            if let Some(frame) = self.parse_frame()? {
                // This indicates a successful read. Quit the loop.
                return Ok(Some(frame));
            }

            // Step 2: we are here means the buffer does not contain a full frame.
            // try to read from network and re-run Step 1
            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
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

    fn parse_frame(&mut self) -> Result<Option<Frame>, Box<dyn Error>> {
        // Step 3: try to parse a full frame
        // Input: a reference to Connection object, allowing us to modify its buffer.
        // Output: The final result shall be either Ok(Some(Frame)) for a successful read,
        // Ok(None) for an ongoing reading as in the continue logic, or Error of a certain kind.
        /*let Ok(()) == self.buffer.Frame::check()?;
        if let Some(frame) == self.buffer.Frame::parse()? {
            Ok(Some(frame))
        }*/
        unimplemented!("Byte parsing logic deferred to Task C data boundary")
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
}
