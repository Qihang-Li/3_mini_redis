use super::frame::Frame;
use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub enum Error {
    EndOfStream,
    Other(&'static str),
}

impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::EndOfStream => write!(f, "command ended early"),
            Error::Other(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug)]
pub struct Parse {
    parts: std::vec::IntoIter<Frame>,
}

impl Parse {
    /// Create a new Parse object
    ///
    /// # Errors
    /// Returns `Error::Other` if the frame is not an `Frame::Array`.
    pub fn new(frame: Frame) -> Result<Parse, Error> {
        match frame {
            Frame::Array(vec) => Ok(Self {
                parts: vec.into_iter(),
            }),
            _ => Err(Error::Other(
                "Wrong message: Invalid command, Frame::Array expected",
            )),
        }
    }

    /// Extract the next byte if available.
    ///
    /// # Errors
    /// Returns `Error::EndOfStream` if there is no next frame
    /// Returns `Error::Other` if the frame is not a simple or bulk string
    pub fn next_bytes(&mut self) -> Result<Bytes, Error> {
        let Some(frame) = self.parts.next() else {
            return Err(Error::EndOfStream);
        };
        match frame {
            Frame::Bulk(bytes) => Ok(bytes),
            Frame::Simple(string) => Ok(Bytes::from(string)),
            _ => Err(Error::Other(
                "Wrong message: Invalid command, Frame::Bulk expected",
            )),
        }
    }

    /// Extract the next byte and convert to string if available.
    ///
    /// # Errors
    /// Returns `Error::Other` if the contents are not UTF-8 compatible
    pub fn next_string(&mut self) -> Result<String, Error> {
        let bytes = self.next_bytes()?;
        let result = String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::Other("Wrong message: Invalid command, UTF-8 incompatible"))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_new() {
        // Test 1: valid command
        let valid_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
        ]);
        let valid_command = Parse::new(valid_frame);
        assert!(valid_command.is_ok());

        // Test 2: invalid command, not as an array
        let invalid_frame = Frame::Null;
        let invalid_command = Parse::new(invalid_frame);
        assert!(matches!(invalid_command, Err(Error::Other(_))));
    }

    #[test]
    fn test_parse_next_bytes() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("Hello, ")),
            Frame::Simple(String::from("World!")),
            Frame::Array(vec![
                Frame::Integer(137i64),
                Frame::Error(String::from("HTTP 504")),
            ]),
        ]);
        let mut command = Parse::new(frame).unwrap();

        // Test 1: valid byte, as a bulk string
        let valid_bulk_byte = command.next_bytes();
        assert_eq!(valid_bulk_byte, Ok(Bytes::from("Hello, ")));

        // Test 2: valid byte, as a simple string
        let valid_simple_byte = command.next_bytes();
        assert_eq!(valid_simple_byte, Ok(Bytes::from("World!")));

        // Test 3: invalid byte, not as a string
        let invalid_byte = command.next_bytes();
        assert!(matches!(invalid_byte, Err(Error::Other(_))));

        // Test 4: empty
        let empty_byte = command.next_bytes();
        assert_eq!(empty_byte, Err(Error::EndOfStream));
    }

    #[test]
    fn test_parse_next_string() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("Hello, ")),
            Frame::Simple(String::from("Tokio!")),
            Frame::Bulk(Bytes::from_static(b"\xF5\xFF")),
        ]);
        let mut command = Parse::new(frame).unwrap();

        // Test 1: valid string, as a bulk string
        let valid_bulk_string = command.next_string();
        assert_eq!(valid_bulk_string, Ok(String::from("Hello, ")));

        // Test 2: valid string, as a simple string
        let valid_simple_string = command.next_string();
        assert_eq!(valid_simple_string, Ok(String::from("Tokio!")));

        // Test 3: invalid string, not UFT-8 compatible
        let invalid_string = command.next_string();
        assert!(matches!(invalid_string, Err(Error::Other(_))));

        // No test 4: no need to check invalid byte and empty vector,
        // since they are verified by test_parse_next_bytes()
        let empty_string = command.next_string();
        assert_eq!(empty_string, Err(Error::EndOfStream));
    }
}
