use crate::connection::Connection;
use crate::frame::Frame;
use bytes::Bytes;
use std::error::Error;
use std::net::SocketAddr;
use std::vec;
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

/// Sends `GET` and `SET` commands over one TCP connection.
///
/// Each command applies `timeout_duration` separately to writing the request
/// and reading the response. These operations do not share one deadline.
///
/// After a command timeout, callers must drop this requester and reconnect.
/// A timeout can leave a partial write or an unread response. This type does
/// not close the connection or prevent further calls after a timeout.
#[derive(Debug)]
pub struct Requester {
    connection: Connection,
    timeout_duration: Duration,
}

impl Requester {
    /// Creates a requester connected to the given server address.
    ///
    /// Applies `timeout_duration` to the connection attempt and stores the same
    /// duration for subsequent request writes and response reads.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection attempt fails or times out.
    pub async fn connect(
        server_addr: SocketAddr,
        timeout_duration: Duration,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Propagate either a timeout or a connection error.
        let socket = timeout(timeout_duration, TcpStream::connect(server_addr)).await??;

        Ok(Self {
            connection: Connection::new(socket),
            timeout_duration,
        })
    }

    /// Fetches the value associated with the given key from the server.
    ///
    /// Returns `Ok(Some(value))` for a bulk response, including an empty value.
    /// Returns `Ok(None)` for a null response, indicating a missing key.
    ///
    /// Drop this requester and reconnect if the operation times out.
    ///
    /// # Errors
    ///
    /// Returns an error if writing or reading fails or times out, the server
    /// closes its sending side before a complete response arrives, or the
    /// response is malformed or exceeds an input limit.
    ///
    /// Also returns an error for a server error frame or an unexpected response
    /// type.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Box<dyn Error + Send + Sync>> {
        // Step 1: Build the command frame
        let command_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"GET")),
            // Copy the borrowed key into an owned bulk frame.
            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
        ]);

        // Step 2: Write and flush the command frame with a timeout
        timeout(
            self.timeout_duration,
            self.connection.write_frame(&command_frame),
        )
        .await??;

        // Step 3: Read the response frame with a fresh timeout
        let Some(response_frame) =
            // Complete response frame on success
            timeout(self.timeout_duration, self.connection.read_frame()).await??
        else {
            // Server closed its sending side without a response
            return Err("Server closed its sending side before a response arrived".into());
        };

        // Step 4: Interpret the response frame
        match response_frame {
            Frame::Bulk(bytes) => Ok(Some(bytes)),
            Frame::Null => Ok(None),
            Frame::Error(string) => Err(string.into()),
            _ => Err("Unexpected response frame type".into()),
        }
    }

    /// Assigns the given value to the specified key on the server.
    ///
    /// Returns `Ok(())` only for a simple-string response containing exactly
    /// `"OK"`.
    ///
    /// A timeout does not establish whether the server applied the command.
    /// Drop this requester and reconnect after a timeout; reconnecting does not
    /// determine the outcome of the previous command.
    ///
    /// # Errors
    ///
    /// Returns an error if writing or reading fails or times out, the server
    /// closes its sending side before a complete response arrives, or the
    /// response is malformed or exceeds an input limit.
    ///
    /// Also returns an error for a server error frame, a simple-string response
    /// other than `"OK"`, or an unexpected response type.
    pub async fn set(
        &mut self,
        key: &str,
        value: Bytes,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Step 1: Build the command frame
        let command_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            // Copy the borrowed key into an owned bulk frame.
            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            Frame::Bulk(value),
        ]);

        // Step 2: Write and flush the command frame with a timeout
        timeout(
            self.timeout_duration,
            self.connection.write_frame(&command_frame),
        )
        .await??;

        // Step 3: Read the response frame with a fresh timeout
        let Some(response_frame) =
            // Complete response frame on success
            timeout(self.timeout_duration, self.connection.read_frame()).await??
        else {
            // Server closed its sending side without a response
            return Err("Server closed its sending side before a response arrived".into());
        };

        // Step 4: Interpret the response frame
        match response_frame {
            Frame::Simple(string) => {
                if string == "OK" {
                    Ok(())
                } else {
                    Err(string.into())
                }
            }
            Frame::Error(string) => Err(string.into()),
            _ => Err("Unexpected response frame type".into()),
        }
    }
}
