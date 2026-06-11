use bytes::Bytes;

pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
}

impl Frame {
    /// Checks if a full frame is available.
    ///
    /// # Errors
    /// Returns `Error::Incomplete` if the byte stream is not a full frame.
    pub fn check(_src: &mut std::io::Cursor<&[u8]>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Parses a frame from the cursor.
    ///
    /// # Errors
    /// Returns an error if the frame has invalid formatting.
    pub fn parse(_src: &mut std::io::Cursor<&[u8]>) -> Result<Frame, Error> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(String),
}

impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Incomplete => write!(f, "stream ended early"),
            Error::Other(err) => write!(f, "{err}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_init() {
        let _frames: [Frame; 6] = [
            Frame::Simple(String::from("Hello, World!")),
            Frame::Error(String::from("404 Not Found")),
            Frame::Integer(42i64),
            Frame::Bulk(Bytes::from_static(b"Hello,\nWorld!!")),
            Frame::Array(vec![
                Frame::Simple(String::from("Hello, World, Again!!!")),
                Frame::Error(String::from("500 Internal Server Error")),
                Frame::Integer(1 << 42),
            ]),
            Frame::Null,
        ];
    }
}
