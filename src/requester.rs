use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use std::error::Error;
use std::net::SocketAddr;
use std::vec;
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

#[derive(Debug)]
pub struct Requester {
    connection: Connection,
    timeout_duration: Duration,
}

impl Requester {
    /// Establishes a TCP connection to input address and initializes Requester.
    ///
    /// # Errors
    /// Returns an error if the underlying OS network stack fails to establish a
    /// connection, or if the 3-way TCP handshake exceeds the `timeout_duration`.
    pub async fn connect(
        ip_addr: SocketAddr,
        timeout_duration: Duration,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match timeout(timeout_duration, TcpStream::connect(ip_addr)).await {
            // (i) connection success
            Ok(Ok(socket)) => Ok(Self {
                connection: Connection::new(socket),
                timeout_duration,
            }),
            // (ii) connection failure
            Ok(Err(error)) => Err(error.into()),
            // (iii) timeout
            Err(elapsed) => Err(elapsed.into()),
        } // exhaustive, but so weird
    }

    /// Fetches the value associated with the given key from the server.
    ///
    /// # Errors
    /// Returns an error if the network transmission fails, if server response
    /// cannot be parsed as a valid RESP Frame, or if the operation times out.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Box<dyn Error + Send + Sync>> {
        // Step 1: generate the command frame
        let command_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"GET")),
            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            // We can also use this below, but it's less efficient due to allocation
            // Frame::Bulk(Bytes::from(key.to_string()))
        ]);

        // Step 2: send the command frame, guaranteed with timeout
        match timeout(
            self.timeout_duration,
            self.connection.write_frame(&command_frame),
        )
        .await
        {
            // 2.(i) command frame sent successfully
            Ok(Ok(())) => (),
            // 2.(ii) command frame sent failure
            Ok(Err(error)) => return Err(error),
            // 2.(iii) timeout
            Err(elapsed) => return Err(elapsed.into()),
        }

        // Step 3: get the response frame, guaranteed with timeout
        let response_frame =
            match timeout(self.timeout_duration, self.connection.read_frame()).await {
                // 3.(i) response frame received successfully
                Ok(Ok(Some(frame))) => frame,
                // 3.(ii) client disconnected cleanly
                Ok(Ok(None)) => return Err("Client has disconnected".into()),
                // 3.(iii) client disconnected failure
                Ok(Err(error)) => return Err(error),
                // 3.(iv) timeout
                Err(elapsed) => return Err(elapsed.into()),
            };

        // Step 4: Break down the response frame
        match response_frame {
            Frame::Bulk(bytes) => Ok(Some(bytes)),
            Frame::Null => Ok(None),
            Frame::Error(string) => Err(string.into()),
            _ => Err("Protocol Error: Unexpected frame type".into()),
        }
    }

    /// Assigns the given value to the specified key on the server.
    ///
    /// # Errors
    /// Returns an error if the network transmission fails, if the server
    /// returns an error frame, or if the operation times out.
    pub async fn set(
        &mut self,
        key: &str,
        value: Bytes,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: generate the command frame
        let command_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            Frame::Bulk(value),
        ]);

        // Step 2: send the command frame, guaranteed with timeout
        match timeout(
            self.timeout_duration,
            self.connection.write_frame(&command_frame),
        )
        .await
        {
            // 2.(i) command frame sent successfully
            Ok(Ok(())) => (),
            // 2.(ii) command frame sent failure
            Ok(Err(error)) => return Err(error),
            // 2.(iii) timeout
            Err(elapsed) => return Err(elapsed.into()),
        }

        // Step 3: get the response frame, guaranteed with timeout
        let response_frame =
            match timeout(self.timeout_duration, self.connection.read_frame()).await {
                // 3.(i) response frame received successfully
                Ok(Ok(Some(frame))) => frame,
                // 3.(ii) client disconnected cleanly
                Ok(Ok(None)) => return Err("Client has disconnected".into()),
                // 3.(iii) client disconnected failure
                Ok(Err(error)) => return Err(error),
                // 3.(iv) timeout
                Err(elapsed) => return Err(elapsed.into()),
            };

        // Step 4: Break down the response frame
        match response_frame {
            Frame::Simple(string) => {
                if string == "OK" {
                    Ok(())
                } else {
                    Err(string.into())
                }
            }
            Frame::Error(string) => Err(string.into()),
            _ => Err("Protocol Error: Unexpected frame type".into()),
        }
    }
}
