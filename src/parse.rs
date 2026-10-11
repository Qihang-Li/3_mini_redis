use crate::frame::Frame;
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
            Error::EndOfStream => write!(f, "No more command arguments"),
            Error::Other(message) => write!(f, "{message}"),
        }
    }
}

#[derive(Debug)]
pub struct Parse {
    parts: std::vec::IntoIter<Frame>,
}

impl Parse {
    /// Creates an argument parser from an array frame.
    ///
    /// Takes ownership of the array elements without validating a command.
    ///
    /// # Errors
    /// Returns `Error::Other` if the input is not a `Frame::Array`.
    pub fn from_frame(frame: Frame) -> Result<Parse, Error> {
        match frame {
            Frame::Array(elements) => Ok(Self {
                parts: elements.into_iter(),
            }),
            _ => Err(Error::Other("Expected an array frame")),
        }
    }

    /// Consumes the next argument and returns its contents as `Bytes`.
    ///
    /// Accepts a bulk or simple string. An argument with an unsupported frame
    /// type is also consumed before the error is returned.
    ///
    /// # Errors
    /// Returns `Error::EndOfStream` if no argument remains.
    /// Returns `Error::Other` if the next argument is not a bulk or simple
    /// string.
    pub fn next_bytes(&mut self) -> Result<Bytes, Error> {
        let Some(frame) = self.parts.next() else {
            return Err(Error::EndOfStream);
        };
        match frame {
            Frame::Bulk(bytes) => Ok(bytes),
            Frame::Simple(string) => Ok(Bytes::from(string)),
            _ => Err(Error::Other("Expected a bulk or simple string argument")),
        }
    }

    /// Consumes the next argument and returns its contents as a `String`.
    ///
    /// Accepts a bulk or simple string containing valid UTF-8. If an argument
    /// is present, it is consumed even when its type or encoding is rejected.
    ///
    /// # Errors
    /// Returns `Error::EndOfStream` if no argument remains.
    /// Returns `Error::Other` if the next argument is not a bulk or simple
    /// string, or if its contents are not valid UTF-8.
    pub fn next_string(&mut self) -> Result<String, Error> {
        let bytes = self.next_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Other("Argument is not valid UTF-8"))
    }

    /// Checks that no arguments remain.
    ///
    /// Returns `Ok(())` when the iterator is exhausted. Otherwise, consumes
    /// one unexpected argument and returns an error.
    ///
    /// # Errors
    /// Returns `Error::Other` if an argument remains.
    pub fn finish(&mut self) -> Result<(), Error> {
        if self.parts.next().is_some() {
            return Err(Error::Other("Unexpected extra argument"));
        }
        Ok(())
    }
}

#[cfg(test)]
impl Parse {
    /// Creates an argument parser from frames for use in tests.
    pub(crate) fn new_test(frames: Vec<Frame>) -> Self {
        Self {
            parts: frames.into_iter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_from_frame() {
        // Array input
        let valid_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
        ]);
        let valid_parser_result = Parse::from_frame(valid_frame);
        assert!(valid_parser_result.is_ok());

        // Non-array input
        let invalid_frame = Frame::Null;
        let invalid_parser_result = Parse::from_frame(invalid_frame);
        assert!(matches!(invalid_parser_result, Err(Error::Other(_))));
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
        let mut parser = Parse::from_frame(frame).unwrap();

        // Bulk-string argument
        let bulk_result = parser.next_bytes();
        assert_eq!(bulk_result, Ok(Bytes::from("Hello, ")));

        // Simple-string argument
        let simple_result = parser.next_bytes();
        assert_eq!(simple_result, Ok(Bytes::from("World!")));

        // Unsupported argument type
        let invalid_result = parser.next_bytes();
        assert!(matches!(invalid_result, Err(Error::Other(_))));

        // Exhausted argument iterator
        let exhausted_result = parser.next_bytes();
        assert_eq!(exhausted_result, Err(Error::EndOfStream));
    }

    #[test]
    fn test_parse_next_string() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("Hello, ")),
            Frame::Simple(String::from("Tokio!")),
            Frame::Bulk(Bytes::from_static(b"\xF5\xFF")),
        ]);
        let mut parser = Parse::from_frame(frame).unwrap();

        // UTF-8 contents in a bulk string
        let bulk_result = parser.next_string();
        assert_eq!(bulk_result, Ok(String::from("Hello, ")));

        // Contents of a simple string
        let simple_result = parser.next_string();
        assert_eq!(simple_result, Ok(String::from("Tokio!")));

        // Invalid UTF-8 contents
        let invalid_result = parser.next_string();
        assert!(matches!(invalid_result, Err(Error::Other(_))));

        // Exhausted argument iterator
        let exhausted_result = parser.next_string();
        assert_eq!(exhausted_result, Err(Error::EndOfStream));
    }

    #[test]
    fn test_parse_finish() {
        let mut parser = Parse::from_frame(Frame::Array(vec![Frame::Null])).unwrap();

        // Unexpected remaining argument
        let error_result = parser.finish();
        assert!(matches!(error_result, Err(Error::Other(_))));

        // Exhausted iterator
        let _ = parser.parts.next();
        let result = parser.finish();
        assert!(result.is_ok());
    }
}
