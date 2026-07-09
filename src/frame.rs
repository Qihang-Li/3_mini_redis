use bytes::Buf;
use bytes::Bytes;
use std::io::Cursor;

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(&'static str),
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

#[derive(Debug, PartialEq)]
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
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        // Input: src as a Cursor to a buffer, allowing us to modify the buffer.
        // Output: either Ok() showing a full frame, or Error of a kind

        // Step 1: check if the cursor is pointing at an empty buffer
        if !src.has_remaining() {
            // this is an empty line []
            return Err(Error::Incomplete);
        }

        // Step 2: match the first byte
        let first_byte = src.get_u8();
        match first_byte {
            // Step 3: deal with simple, error, or integer
            b'+' | b'-' | b':' => {
                let _bytes = Frame::get_line(src)?;
                // this is a valid simple, error, or integer
                Ok(())
            }

            // Step 4: deal with bulk
            b'$' => {
                // Step 4.1: get length of bulk string
                let length = Frame::get_decimal(src)?;
                // Step 4.2: match length of bulk string
                match length {
                    -1 => {
                        // this is a null bulk string
                        Ok(())
                    }
                    l if l >= 0 => {
                        let length_u = usize::try_from(length)
                            .map_err(|_| Error::Other("Wrong message: Length overflow"))?;
                        // Step 4.3 check remaining buffer length
                        if src.remaining() >= length_u {
                            // Step 4.4: advance cursor and compare to \r\n
                            src.advance(length_u);
                            if src.get_u8() == 13 && src.get_u8() == 10 {
                                // this is a valid bulk string
                                return Ok(());
                            }
                            // this is an invalid bulk string not ending with \r\n
                            return Err(Error::Other(
                                "Wrong message: Invalid ending for bulk string",
                            ));
                        }
                        // this is an incomplete bulk string
                        Err(Error::Incomplete)
                    }
                    _ => {
                        // this is a bulk string with negative length
                        Err(Error::Other(
                            "Wrong message: Invalid length for bulk string",
                        ))
                    }
                }
            }

            // Step 5: deal with array
            b'*' => {
                // Step 5.1: get size of array
                let size = Frame::get_decimal(src)?;
                // Step 5.2: match size of array
                match size {
                    -1 => {
                        // this is a null array
                        Ok(())
                    }
                    s if s >= 0 => {
                        // Step 5.3 deal with recursion
                        for _ in 0..size {
                            Frame::check(src)?;
                            // if we have a interstitial fragmented array here,
                            // the "next inner frame" shall be [], and trigger
                            // Err(Incomplete) by the first line in check()
                        }
                        Ok(())
                    }
                    _ => {
                        // this is a array with negative length
                        Err(Error::Other("Wrong message: Invalid size for array"))
                    }
                }
            }

            _ => {
                // this is a frame not starting with =-:$*
                Err(Error::Other("Wrong message: Invalid first byte"))
            }
        }
    }

    /// Parses a frame from the cursor.
    ///
    /// # Errors
    /// Returns an error if the frame has invalid formatting.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        // Input: src as a Cursor to a buffer, allowing us to modify the buffer.
        // Output: either Ok(Frame) for a full frame, or Error of a kind

        // Step 1: match the first byte
        let first_byte = src.get_u8();
        match first_byte {
            // Step 2: deal with simple
            b'+' | b'-' => {
                let content = Frame::get_line(src)?;
                let result = String::from_utf8(content.to_vec())
                    .map_err(|_| Error::Other("Wrong message: Invalid UTF-8"))?;
                // this is a valid simple or error
                if first_byte == b'+' {
                    Ok(Frame::Simple(result))
                } else {
                    Ok(Frame::Error(result))
                }
            }

            // Step 3: deal with simple, error, or integer
            b':' => {
                let result = Frame::get_decimal(src)?;
                // this is a valid simple, error, or integer
                Ok(Frame::Integer(result))
            }

            // Step 4: deal with bulk
            b'$' => {
                // Step 4.1: get length of bulk string
                let length = Frame::get_decimal(src)?;
                // Step 4.2: match length of bulk string
                if length >= 0 {
                    let length_u = usize::try_from(length)
                        .map_err(|_| Error::Other("Wrong message: Length overflow"))?;
                    // Step 4.3 collect the output
                    let result = Bytes::copy_from_slice(&src.chunk()[..length_u]);
                    // move cursor forward by length + 2
                    src.advance(length_u + 2);
                    // this is an valid bulk string
                    return Ok(Frame::Bulk(result));
                }
                // this is a null bulk string
                Ok(Frame::Null)
            }

            // Step 5: deal with array
            b'*' => {
                // Step 5.1: get size of array
                let size = Frame::get_decimal(src)?;
                // Step 5.2: match size of array
                if size >= 0 {
                    let size_u = usize::try_from(size)
                        .map_err(|_| Error::Other("Wrong message: Length overflow"))?;
                    if size_u >= 1024 {
                        return Err(Error::Other("Wrong message: size out of memory"));
                    }
                    // Step 5.3 deal with recursion
                    let mut result = Vec::with_capacity(size_u);
                    for _ in 0..size {
                        result.push(Frame::parse(src)?);
                    }
                    return Ok(Frame::Array(result));
                }
                // this is a null array
                Ok(Frame::Null)
            }

            _ => {
                // a cursor has passed check() should never reach here
                Err(Error::Other("Wrong message: Invalid first byte"))
            }
        }
    }

    fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
        // Input: src as a Cursor to a buffer, allowing us to modify the buffer.
        // Output: either Ok(&[u8]) as the contents to the buffer, or Error

        // Step 1: get the current content and position of the Cursor
        let line = src.chunk();
        let pos = usize::try_from(src.position())
            .map_err(|_| Error::Other("Wrong message: Length overflow"))?;

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
                    return Err(Error::Other("Wrong message: '\r' not followed by '\n'"));
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
            return Err(Error::Other("Wrong message: Empty line"));
        }
        let mut is_pos = 1i64;
        let mut result = 0i64;

        // Step 3: deal with possible "-" signs in the beginning
        let first_byte = line.get_u8();
        match first_byte {
            45 => is_pos = -1,
            48..=57 => result = i64::from(first_byte - 48),
            _ => {
                return Err(Error::Other("Wrong message: Not a number"));
            }
        }
        if !line.has_remaining() && (is_pos == -1) {
            // This is nothing but a single "-\r\n"
            return Err(Error::Other("Wrong message: Not a number"));
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
                    return Err(Error::Other("Wrong message: Not a number"));
                }
            }
        }
        Ok(is_pos * result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_new() {
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
    fn test_frame_get_line() {
        // Test 1: Valid clean line
        let valid_line = &b"hello world\r\n"[..];
        let mut valid_cursor = Cursor::new(valid_line);
        let valid_bytes = Frame::get_line(&mut valid_cursor);
        assert_eq!(valid_bytes.unwrap(), b"hello world");
        assert_eq!(valid_cursor.position(), 13);

        // Test 2: Superfluous pipelined data
        let superfluous_line = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_line);
        let superfluous_bytes = Frame::get_line(&mut superfluous_cursor);
        assert_eq!(superfluous_bytes.unwrap(), b"2");
        assert_eq!(superfluous_cursor.position(), 3);

        // Test 3: Inadequate line
        let inadequate_line = &b"Lorem Ipsum"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_line);
        let inadequate_bytes = Frame::get_line(&mut inadequate_cursor);
        assert!(matches!(inadequate_bytes, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Test 4: Wrong line
        let wrong_line = &b"dolor \rsit"[..];
        let mut wrong_cursor = Cursor::new(wrong_line);
        let wrong_bytes = Frame::get_line(&mut wrong_cursor);
        assert!(matches!(wrong_bytes, Err(Error::Other(_))));
        assert_eq!(wrong_cursor.position(), 0);
    }

    #[test]
    fn test_frame_get_decimal() {
        // Test 1: Valid positive number
        let valid_pos = &b"42\r\n"[..];
        let mut valid_pos_cursor = Cursor::new(valid_pos);
        let valid_pos_int = Frame::get_decimal(&mut valid_pos_cursor);
        assert_eq!(valid_pos_int.unwrap(), 42i64);
        assert_eq!(valid_pos_cursor.position(), 4);

        // Test 2: Valid negative number
        let valid_neg = &b"-137\r\n"[..];
        let mut valid_neg_cursor = Cursor::new(valid_neg);
        let valid_neg_int = Frame::get_decimal(&mut valid_neg_cursor);
        assert_eq!(valid_neg_int.unwrap(), -137i64);
        assert_eq!(valid_neg_cursor.position(), 6);

        // Test 3: Valid single digit number
        let valid_sig = &b"9\r\n"[..];
        let mut valid_sig_cursor = Cursor::new(valid_sig);
        let valid_sig_int = Frame::get_decimal(&mut valid_sig_cursor);
        assert_eq!(valid_sig_int.unwrap(), 9i64);
        assert_eq!(valid_sig_cursor.position(), 3);

        // Test 4: Superfluous pipelined data
        let superfluous_num = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_num);
        let superfluous_int = Frame::get_decimal(&mut superfluous_cursor);
        assert_eq!(superfluous_int.unwrap(), 2i64);
        assert_eq!(superfluous_cursor.position(), 3);

        // Test 5: Inadequate line
        let inadequate_num = &b"299792458"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_num);
        let inadequate_int = Frame::get_decimal(&mut inadequate_cursor);
        assert!(matches!(inadequate_int, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Test 6: Non-integer line
        let non_num = &b"No. 1729\r\n"[..];
        let mut non_cursor = Cursor::new(non_num);
        let non_int = Frame::get_decimal(&mut non_cursor);
        assert!(matches!(non_int, Err(Error::Other(_))));
        assert_eq!(non_cursor.position(), 10);

        // Test 7: Only minus sign line
        let only_min = &b"-\r\n"[..];
        let mut min_cursor = Cursor::new(only_min);
        let min_int = Frame::get_decimal(&mut min_cursor);
        assert!(matches!(min_int, Err(Error::Other(_))));
        assert_eq!(min_cursor.position(), 3);

        // Test 8: Unexpected minus sign line
        let wrong_min = &b"42-137\r\n"[..];
        let mut wrong_cursor = Cursor::new(wrong_min);
        let wrong_int = Frame::get_decimal(&mut wrong_cursor);
        assert!(matches!(wrong_int, Err(Error::Other(_))));
        assert_eq!(wrong_cursor.position(), 8);
    }

    #[test]
    fn test_frame_check() {
        // Test 1: Valid simple string
        let valid_simple = &b"+Hello, world!\r\n"[..];
        let mut simple_cursor = Cursor::new(valid_simple);
        let simple_result = Frame::check(&mut simple_cursor);
        assert!(simple_result.is_ok());
        assert_eq!(simple_cursor.position(), 16);

        // Test 2: Valid error
        let valid_error = &b"-Error 404 Not Found\r\n"[..];
        let mut error_cursor = Cursor::new(valid_error);
        let error_result = Frame::check(&mut error_cursor);
        assert!(error_result.is_ok());
        assert_eq!(error_cursor.position(), 22);

        // Test 3: Valid integer
        let valid_integer = &b":42\r\n"[..];
        let mut integer_cursor = Cursor::new(valid_integer);
        let integer_result = Frame::check(&mut integer_cursor);
        assert!(integer_result.is_ok());
        assert_eq!(integer_cursor.position(), 5);

        // Test 4: Valid bulk string
        let valid_bulk = &b"$6\r\nfoobar\r\n"[..];
        let mut bulk_cursor = Cursor::new(valid_bulk);
        let bulk_result = Frame::check(&mut bulk_cursor);
        assert!(bulk_result.is_ok());
        assert_eq!(bulk_cursor.position(), 12);

        // Test 5: Valid empty bulk string
        let valid_emptybulk = &b"$0\r\n\r\n"[..];
        let mut emptybulk_cursor = Cursor::new(valid_emptybulk);
        let emptybulk_result = Frame::check(&mut emptybulk_cursor);
        assert!(emptybulk_result.is_ok());
        assert_eq!(emptybulk_cursor.position(), 6);

        // Test 6: Valid null bulk string
        let valid_nullbulk = &b"$-1\r\n"[..];
        let mut nullbulk_cursor = Cursor::new(valid_nullbulk);
        let nullbulk_result = Frame::check(&mut nullbulk_cursor);
        assert!(nullbulk_result.is_ok());
        assert_eq!(nullbulk_cursor.position(), 5);

        // Test 7: Inadequate bulk string
        let inadequate_bulk = &b"$6\r\nfoo"[..];
        let mut inadequate_bulk_cursor = Cursor::new(inadequate_bulk);
        let inadequate_bulk_result = Frame::check(&mut inadequate_bulk_cursor);
        assert!(matches!(inadequate_bulk_result, Err(Error::Incomplete)));
        // get_decimal() advances cursor at f
        assert_eq!(inadequate_bulk_cursor.position(), 4);

        // Test 8: Valid array
        let valid_array = &b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut array_cursor = Cursor::new(valid_array);
        let array_result = Frame::check(&mut array_cursor);
        assert!(array_result.is_ok());
        assert_eq!(array_cursor.position(), 22);

        // Test 9: Valid nested array
        let valid_nestarray = &b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"[..];
        let mut nestarray_cursor = Cursor::new(valid_nestarray);
        let nestarray_result = Frame::check(&mut nestarray_cursor);
        assert!(nestarray_result.is_ok());
        assert_eq!(nestarray_cursor.position(), 36);

        // Test 10: Valid empty array
        let valid_emptyarray = &b"*0\r\n"[..];
        let mut emptyarray_cursor = Cursor::new(valid_emptyarray);
        let emptyarray_result = Frame::check(&mut emptyarray_cursor);
        assert!(emptyarray_result.is_ok());
        assert_eq!(emptyarray_cursor.position(), 4);

        // Test 11: Valid null array
        let valid_nullarray = &b"*-1\r\n"[..];
        let mut nullarray_cursor = Cursor::new(valid_nullarray);
        let nullarray_result = Frame::check(&mut nullarray_cursor);
        assert!(nullarray_result.is_ok());
        assert_eq!(nullarray_cursor.position(), 5);

        // Test 12: Interstitial fragmented array
        let cutoff_array = &b"*2\r\n$3\r\nfoo\r\n"[..];
        let mut cutoff_cursor = Cursor::new(cutoff_array);
        let cutoff_result = Frame::check(&mut cutoff_cursor);
        assert!(matches!(cutoff_result, Err(Error::Incomplete)));
        assert_eq!(cutoff_cursor.position(), 13);

        // Test 13: Empty data
        let empty_data = &b""[..];
        let mut empty_cursor = Cursor::new(empty_data);
        let empty_result = Frame::check(&mut empty_cursor);
        assert!(matches!(empty_result, Err(Error::Incomplete)));
        assert_eq!(empty_cursor.position(), 0);

        // Test 14: Invalid first byte
        let wrong_1stbyte = &b"&hello world\r\n"[..];
        let mut wrong_1stbyte_cursor = Cursor::new(wrong_1stbyte);
        let wrong_1stbyte_result = Frame::check(&mut wrong_1stbyte_cursor);
        assert!(matches!(wrong_1stbyte_result, Err(Error::Other(_))));
        assert_eq!(wrong_1stbyte_cursor.position(), 1);

        // Test 15: Invalid bulk length
        let wrong_bulklen = &b"$-42\r\n"[..];
        let mut wrong_bulklen_cursor = Cursor::new(wrong_bulklen);
        let wrong_bulklen_result = Frame::check(&mut wrong_bulklen_cursor);
        assert!(matches!(wrong_bulklen_result, Err(Error::Other(_))));
        assert_eq!(wrong_bulklen_cursor.position(), 6);

        // Test 16: Invalid bulk string
        let wrong_bulk = &b"$6\r\nfoobar\r3"[..];
        let mut wrong_bulk_cursor = Cursor::new(wrong_bulk);
        let wrong_bulk_result = Frame::check(&mut wrong_bulk_cursor);
        assert!(matches!(wrong_bulk_result, Err(Error::Other(_))));
        assert_eq!(wrong_bulk_cursor.position(), 12);

        // Test 17: Invalid array size
        let wrong_arraysize = &b"*-137\r\n"[..];
        let mut wrong_arraysize_cursor = Cursor::new(wrong_arraysize);
        let wrong_arraysize_result = Frame::check(&mut wrong_arraysize_cursor);
        assert!(matches!(wrong_arraysize_result, Err(Error::Other(_))));
        assert_eq!(wrong_arraysize_cursor.position(), 7);
    }

    #[test]
    fn test_frame_parse() {
        // Test 1: Valid simple string
        let valid_simple = &b"+Hello, World!\r\n"[..];
        let mut simple_cursor = Cursor::new(valid_simple);
        let simple_frame = Frame::parse(&mut simple_cursor);
        assert_eq!(
            simple_frame.unwrap(),
            Frame::Simple("Hello, World!".to_string())
        );
        assert_eq!(simple_cursor.position(), 16);

        // Test 2: Valid error
        let valid_error = &b"-Error 404 Not Found\r\n"[..];
        let mut error_cursor = Cursor::new(valid_error);
        let error_frame = Frame::parse(&mut error_cursor);
        assert_eq!(
            error_frame.unwrap(),
            Frame::Error("Error 404 Not Found".to_string())
        );
        assert_eq!(error_cursor.position(), 22);

        // Test 3: Valid integer
        let valid_integer = &b":42\r\n"[..];
        let mut integer_cursor = Cursor::new(valid_integer);
        let integer_frame = Frame::parse(&mut integer_cursor);
        assert_eq!(integer_frame.unwrap(), Frame::Integer(42i64));
        assert_eq!(integer_cursor.position(), 5);

        // Test 4: Valid bulk string
        let valid_bulk = &b"$6\r\nfoobar\r\n"[..];
        let mut bulk_cursor = Cursor::new(valid_bulk);
        let bulk_frame = Frame::parse(&mut bulk_cursor);
        assert_eq!(bulk_frame.unwrap(), Frame::Bulk("foobar".as_bytes().into()));
        assert_eq!(bulk_cursor.position(), 12);

        // Test 5: Valid empty bulk string
        let valid_emptybulk = &b"$0\r\n\r\n"[..];
        let mut emptybulk_cursor = Cursor::new(valid_emptybulk);
        let emptybulk_frame = Frame::parse(&mut emptybulk_cursor);
        assert_eq!(emptybulk_frame.unwrap(), Frame::Bulk("".as_bytes().into()));
        assert_eq!(emptybulk_cursor.position(), 6);

        // Test 6: Valid null bulk string
        let valid_nullbulk = &b"$-1\r\n"[..];
        let mut nullbulk_cursor = Cursor::new(valid_nullbulk);
        let nullbulk_frame = Frame::parse(&mut nullbulk_cursor);
        assert_eq!(nullbulk_frame.unwrap(), Frame::Null);
        assert_eq!(nullbulk_cursor.position(), 5);

        // Test 7: Valid array
        let valid_array = &b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut array_cursor = Cursor::new(valid_array);
        let array_frame = Frame::parse(&mut array_cursor);
        assert_eq!(
            array_frame.unwrap(),
            Frame::Array(vec![
                Frame::Bulk("foo".as_bytes().into()),
                Frame::Bulk("bar".as_bytes().into())
            ])
        );
        assert_eq!(array_cursor.position(), 22);

        // Test 8: Valid nested array
        let valid_nestarray = &b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"[..];
        let mut nestarray_cursor = Cursor::new(valid_nestarray);
        let nestarray_frame = Frame::parse(&mut nestarray_cursor);
        assert_eq!(
            nestarray_frame.unwrap(),
            Frame::Array(vec![
                Frame::Array(vec![
                    Frame::Integer(1i64),
                    Frame::Integer(2i64),
                    Frame::Integer(3i64)
                ]),
                Frame::Array(vec![
                    Frame::Simple("Foo".to_string()),
                    Frame::Error("Bar".to_string())
                ]),
            ])
        );
        assert_eq!(nestarray_cursor.position(), 36);

        // Test 9: Valid empty array
        let valid_emptyarray = &b"*0\r\n"[..];
        let mut emptyarray_cursor = Cursor::new(valid_emptyarray);
        let emptyarray_frame = Frame::parse(&mut emptyarray_cursor);
        assert_eq!(emptyarray_frame.unwrap(), Frame::Array(vec![]));
        assert_eq!(emptyarray_cursor.position(), 4);

        // Test 10: Valid null array
        let valid_nullarray = &b"*-1\r\n"[..];
        let mut nullarray_cursor = Cursor::new(valid_nullarray);
        let nullarray_frame = Frame::parse(&mut nullarray_cursor);
        assert_eq!(nullarray_frame.unwrap(), Frame::Null);
        assert_eq!(nullarray_cursor.position(), 5);

        // Test 11: Invalid first byte
        let wrong_1stbyte = &b"&hello world\r\n"[..];
        let mut wrong_1stbyte_cursor = Cursor::new(wrong_1stbyte);
        let wrong_1stbyte_frame = Frame::parse(&mut wrong_1stbyte_cursor);
        assert!(matches!(wrong_1stbyte_frame, Err(Error::Other(_))));
        // get_u8() advances cursor by 1
        assert_eq!(wrong_1stbyte_cursor.position(), 1);
    }
}
