use bytes::Buf;
use bytes::Bytes;
use std::io::Cursor;

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
    pub fn check(_src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        unimplemented!()
    }

    /// Parses a frame from the cursor.
    ///
    /// # Errors
    /// Returns an error if the frame has invalid formatting.
    pub fn parse(_src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        unimplemented!()
    }

    fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
        // Input: src as a Cursor to a buffer, allowing us to modify the buffer.
        // Output: either Ok(&[u8]) as the contents to the buffer, or Error

        // Step 1: get the current content and position of the Cursor
        let line = src.chunk();
        let pos = usize::try_from(src.position()).unwrap();

        // Step 2: scan the content and try to find the first "\r"
        for index in 0..line.len() {
            if line[index] == 13 {
                // This is a successful search. continue on the rest
                if index + 1 < line.len() {
                    // Step 3: check if "\n" comes right behind the "\r"
                    if line[index + 1] == 10 {
                        // This is for a valid line like "hello world\r\n"
                        // or, a superflous line which contains a valid line

                        // use get_ref() in order not to modify line
                        let result = &src.get_ref()[pos..pos + index];
                        // cut off the already-read bytes
                        src.advance(index + 2);
                        return Ok(result);
                    }
                    // This is for a wrong line like "dolor \rsit"
                    return Err(Error::Other(
                        "Wrong message: '\r' not followed by '\n'".into(),
                    ));
                }
                // This is for an incomplete line like "Lorem Ipsum\r"
                return Err(Error::Incomplete);
            }
        }
        // here is for an incomplete line like "Lorem Ipsum"
        Err(Error::Incomplete)
    }

    fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
        // Input: src as a Cursor to a buffer, allowing us to modify the buffer.
        // Output: either Ok(i64) as the number of the buffer, or Error

        // Step 1: get the slice using get_line, or return corresponding error
        let mut line = Frame::get_line(src)?;

        // Step 2: check if the slice is empty, and set up variables
        if !line.has_remaining() {
            return Err(Error::Other("Wrong message: Empty line".into()));
        }
        let mut is_pos = 1i64;
        let mut result = 0i64;

        // Step 3: deal with possible "-" signs in the beginning
        let first_byte = line.get_u8();
        match first_byte {
            45 => is_pos = -1,
            48..=57 => result = i64::from(first_byte - 48),
            _ => {
                return Err(Error::Other("Wrong message: Not a number".into()));
            }
        }
        if !line.has_remaining() && (is_pos == -1) {
            // This is nothing but a single "-\r\n"
            return Err(Error::Other("Wrong message: Not a number".into()));
        }

        // Step 4: handle the remaining bytes
        while line.has_remaining() {
            result *= 10;
            let byte = line.get_u8();
            match byte {
                48..=57 => {
                    result += i64::from(byte - 48);
                }
                _ => {
                    return Err(Error::Other("Wrong message: Not a number".into()));
                }
            }
        }
        Ok(is_pos * result)
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

    #[test]
    fn test_get_line() {
        // Test 1: Valid clean line
        let valid_line = &b"hello world\r\n"[..];
        let mut valid_cursor = Cursor::new(valid_line);
        let valid_result = Frame::get_line(&mut valid_cursor);
        assert_eq!(valid_result.unwrap(), b"hello world");
        assert_eq!(valid_cursor.position(), 13);

        // Test 2: Superfluous pipelined data
        let superfluous_line = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_line);
        let superfluous_result = Frame::get_line(&mut superfluous_cursor);
        assert_eq!(superfluous_result.unwrap(), b"2");
        assert_eq!(superfluous_cursor.position(), 3);

        // Test 3: Inadequate line
        let inadequate_line = &b"Lorem Ipsum"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_line);
        let inadequate_result = Frame::get_line(&mut inadequate_cursor);
        assert!(matches!(inadequate_result, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Test 4: Wrong line
        let wrong_line = &b"dolor \rsit"[..];
        let mut wrong_cursor = Cursor::new(wrong_line);
        let wrong_result = Frame::get_line(&mut wrong_cursor);
        assert!(matches!(wrong_result, Err(Error::Other(_))));
        assert_eq!(wrong_cursor.position(), 0);
    }

    #[test]
    fn test_get_decimal() {
        // Test 1: Valid positive number
        let valid_pos = &b"42\r\n"[..];
        let mut valid_pos_cursor = Cursor::new(valid_pos);
        let valid_pos_result = Frame::get_decimal(&mut valid_pos_cursor);
        assert_eq!(valid_pos_result.unwrap(), 42i64);
        assert_eq!(valid_pos_cursor.position(), 4);

        // Test 2: Valid negative number
        let valid_neg = &b"-137\r\n"[..];
        let mut valid_neg_cursor = Cursor::new(valid_neg);
        let valid_neg_result = Frame::get_decimal(&mut valid_neg_cursor);
        assert_eq!(valid_neg_result.unwrap(), -137i64);
        assert_eq!(valid_neg_cursor.position(), 6);

        // Test 3: Valid single digit number
        let valid_sig = &b"9\r\n"[..];
        let mut valid_sig_cursor = Cursor::new(valid_sig);
        let valid_sig_result = Frame::get_decimal(&mut valid_sig_cursor);
        assert_eq!(valid_sig_result.unwrap(), 9i64);
        assert_eq!(valid_sig_cursor.position(), 3);

        // Test 4: Superfluous pipelined data
        let superfluous_num = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_num);
        let superfluous_result = Frame::get_decimal(&mut superfluous_cursor);
        assert_eq!(superfluous_result.unwrap(), 2i64);
        assert_eq!(superfluous_cursor.position(), 3);

        // Test 5: Inadequate line
        let inadequate_num = &b"299792458"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_num);
        let inadequate_result = Frame::get_decimal(&mut inadequate_cursor);
        assert!(matches!(inadequate_result, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Test 6: Non-integer line
        let non_num = &b"No. 1729\r\n"[..];
        let mut non_cursor = Cursor::new(non_num);
        let non_result = Frame::get_decimal(&mut non_cursor);
        assert!(matches!(non_result, Err(Error::Other(_))));
        assert_eq!(non_cursor.position(), 10);

        // Test 7: only minus sign line
        let only_min = &b"-\r\n"[..];
        let mut min_cursor = Cursor::new(only_min);
        let min_result = Frame::get_decimal(&mut min_cursor);
        assert!(matches!(min_result, Err(Error::Other(_))));
        assert_eq!(min_cursor.position(), 3);

        // Test 8: unexpected minus sign line
        let wrong_min = &b"42-137\r\n"[..];
        let mut wrong_cursor = Cursor::new(wrong_min);
        let wrong_result = Frame::get_decimal(&mut wrong_cursor);
        assert!(matches!(wrong_result, Err(Error::Other(_))));
        assert_eq!(wrong_cursor.position(), 8);
    }
}
