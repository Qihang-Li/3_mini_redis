use crate::config;
use bytes::{Buf, Bytes};
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
            Error::Incomplete => write!(f, "Incomplete frame"),
            Error::Other(message) => write!(f, "{message}"),
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
    /// Checks one frame's structural completeness at the current position.
    ///
    /// On success, advances the cursor past that frame. Content decoding may
    /// still fail, for example if an integer frame contains nonnumeric text.
    /// The input bytes are not modified.
    ///
    /// # Errors
    /// Returns `Error::Incomplete` if more input is needed.
    /// Returns `Error::Other` for invalid framing, invalid length fields,
    /// or an exceeded array length or nesting limit.
    /// The cursor position after an error is unspecified.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        Self::check_with_depth(src, 0)
    }

    /// Checks one frame while tracking the number of enclosing arrays.
    ///
    /// A top-level frame starts at depth zero. Each array checks its children
    /// at the next depth.
    fn check_with_depth(src: &mut Cursor<&[u8]>, depth: i32) -> Result<(), Error> {
        // Step 1: Check that input remains at the cursor
        if !src.has_remaining() {
            // A missing frame or array element requires more input.
            return Err(Error::Incomplete);
        }

        // Step 2: Read the frame marker and select its format
        let first_byte = src.get_u8();
        match first_byte {
            // Simple strings, error frames, and integer frames
            b'+' | b'-' | b':' => {
                let _bytes = Frame::get_line(src)?;
                // The line is complete; its contents are not decoded here.
                Ok(())
            }

            // Bulk string
            b'$' => {
                // Read the declared payload length.
                let length = Frame::get_decimal(src)?;
                // Handle null, nonnegative, and invalid negative lengths.
                match length {
                    -1 => {
                        // Null bulk string
                        Ok(())
                    }
                    length if length >= 0 => {
                        let length_usize = usize::try_from(length)
                            .map_err(|_| Error::Other("Bulk string length overflow"))?;
                        // Include the two-byte CRLF terminator.
                        let length_required = length_usize
                            .checked_add(2)
                            .ok_or(Error::Other("Bulk string length overflow"))?;
                        // Require the full payload and its terminator.
                        if src.remaining() >= length_required {
                            // Skip the payload, then validate CRLF.
                            src.advance(length_usize);
                            if src.get_u8() == b'\r' && src.get_u8() == b'\n' {
                                // The payload and terminator are complete.
                                return Ok(());
                            }
                            // The payload is complete, but CRLF is invalid.
                            return Err(Error::Other("Invalid bulk string terminator"));
                        }
                        // The payload or its terminator is incomplete.
                        Err(Error::Incomplete)
                    }
                    _ => {
                        // Negative lengths other than `-1` are invalid.
                        Err(Error::Other("Invalid bulk string length"))
                    }
                }
            }

            // Array
            b'*' => {
                // Reject an array that would exceed the nesting limit.
                if depth >= config::MAX_ARRAY_DEPTH {
                    return Err(Error::Other(
                        "Array nesting depth exceeds the supported limit",
                    ));
                }
                // Read the declared number of elements.
                let array_length = Frame::get_decimal(src)?;
                // Handle null, empty, and nonempty arrays.
                match array_length {
                    -1..=0 => {
                        // Null or empty array
                        Ok(())
                    }
                    1..config::ARRAY_LENGTH_LIMIT_EXCLUSIVE => {
                        // Check each child at the next nesting depth.
                        for _ in 0..array_length {
                            // If no bytes remain, this call returns
                            // `Error::Incomplete`.
                            Self::check_with_depth(src, depth + 1)?;
                        }
                        Ok(())
                    }
                    _ => {
                        // Reject lengths below `-1` or at or above the
                        // configured exclusive limit.
                        Err(Error::Other("Array length is outside the supported range"))
                    }
                }
            }

            _ => {
                // Supported markers are `+`, `-`, `:`, `$`, and `*`.
                Err(Error::Other("Invalid frame type marker"))
            }
        }
    }

    /// Parses one frame starting at the current cursor position.
    ///
    /// Performs its own structural check; callers do not need to call
    /// `check` first. On success, advances past the parsed frame and leaves
    /// subsequent bytes unread.
    ///
    /// # Errors
    /// Returns `Error::Incomplete` if more input is needed.
    /// Returns `Error::Other` for invalid input or exceeded resource limits.
    /// The cursor position after an error is unspecified.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let start_position = src.position();

        Self::check(src)?;

        // Rewind to decode the frame from its original start.
        src.set_position(start_position);
        Self::parse_data(src)
    }

    /// Decodes one frame whose structure has already been checked.
    ///
    /// The cursor must be at the start of a frame covered by a successful
    /// `check` call. Array children satisfy this requirement because their
    /// enclosing array was checked recursively.
    ///
    /// # Errors
    /// Returns `Error::Other` if content decoding fails, for example because
    /// of invalid UTF-8 or an integer outside the `i64` range.
    fn parse_data(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        // Read the frame marker and select its format.
        let first_byte = src.get_u8();
        match first_byte {
            // Simple string or error frame
            b'+' | b'-' => {
                let content = Frame::get_line(src)?;
                // Copy the contents into an owned UTF-8 string.
                let string = String::from_utf8(content.to_vec())
                    .map_err(|_| Error::Other("Invalid UTF-8"))?;
                // Select the variant after validating UTF-8.
                if first_byte == b'+' {
                    Ok(Frame::Simple(string))
                } else {
                    Ok(Frame::Error(string))
                }
            }

            // Integer frame
            b':' => {
                let number = Frame::get_decimal(src)?;
                Ok(Frame::Integer(number))
            }

            // Bulk string
            b'$' => {
                // Read the declared payload length.
                let length = Frame::get_decimal(src)?;
                // A null bulk string has no payload.
                if length == -1 {
                    return Ok(Frame::Null);
                }
                let length_usize = usize::try_from(length)
                    .map_err(|_| Error::Other("Bulk string length overflow"))?;
                // Copy the payload into an owned `Bytes` value.
                let bytes = Bytes::copy_from_slice(&src.chunk()[..length_usize]);
                // Skip the payload and its already-checked CRLF terminator.
                src.advance(length_usize + 2);
                Ok(Frame::Bulk(bytes))
            }

            // Array
            b'*' => {
                // Read the declared number of elements.
                let array_length = Frame::get_decimal(src)?;
                // A null array has no elements.
                if array_length == -1 {
                    return Ok(Frame::Null);
                }
                // Decode children already covered by the structural check.
                let mut elements = Vec::with_capacity(
                    usize::try_from(array_length)
                        .map_err(|_| Error::Other("Array length overflow"))?,
                );
                for _ in 0..array_length {
                    elements.push(Frame::parse_data(src)?);
                }
                Ok(Frame::Array(elements))
            }

            _ => {
                // Successful structural checking excludes this branch.
                Err(Error::Other("Invalid frame type marker"))
            }
        }
    }

    /// Reads one CRLF-terminated line at the current cursor position.
    ///
    /// Returns the line contents as a borrowed slice, excluding CRLF.
    /// On success, advances past CRLF without modifying the input bytes.
    /// The returned contents contain neither CR nor LF.
    ///
    /// # Errors
    /// Returns `Error::Incomplete` if the scan reaches the end without finding
    /// CR or LF, or encounters CR as the final available byte.
    /// Returns `Error::Other` if the scan encounters a bare LF, a CR followed
    /// by a byte other than LF, or an unrepresentable cursor position.
    fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
        // Step 1: Borrow the unread bytes and record the cursor position
        let line = src.chunk();
        let start_position = usize::try_from(src.position())
            .map_err(|_| Error::Other("Cursor position cannot be represented as usize"))?;

        // Step 2: Scan for CRLF, rejecting bare line feeds
        for index in 0..line.len() {
            if line[index] == b'\n' {
                return Err(Error::Other("Bare line feed in line"));
            }

            if line[index] != b'\r' {
                continue;
            }

            // A carriage return needs a following byte.
            if index + 1 >= line.len() {
                // The input ends immediately after `\r`.
                return Err(Error::Incomplete);
            }

            // Step 3: Check for the following line feed
            if line[index + 1] != b'\n' {
                // A different byte after the carriage return is malformed.
                return Err(Error::Other("Carriage return is not followed by line feed"));
            }

            // A complete line may be followed by more input.
            // Keep the returned slice tied to the input's lifetime.
            let result = &src.get_ref()[start_position..start_position + index];
            // Advance past CRLF without removing input bytes.
            src.advance(index + 2);
            return Ok(result);
        }
        // No carriage return was found in the available input.
        Err(Error::Incomplete)
    }

    /// Parses one CRLF-terminated decimal integer as an `i64`.
    ///
    /// Accepts ASCII digits with an optional leading `-`, but no leading `+`.
    /// Once `get_line` succeeds, the cursor remains past the line's CRLF even
    /// if numeric decoding fails.
    ///
    /// # Errors
    /// Propagates errors from `get_line`.
    /// Returns `Error::Other` for an empty line, invalid digits or sign,
    /// or a value outside the `i64` range.
    fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
        // Step 1: Read a CRLF-terminated line
        let mut line = Frame::get_line(src)?;

        // Step 2: Reject an empty line and initialize the accumulator
        if !line.has_remaining() {
            return Err(Error::Other("Empty decimal field"));
        }
        let mut sign = 1i64;
        let mut result = 0i64;

        // Step 3: Read an optional minus sign or the first digit
        let first_byte = line.get_u8();
        match first_byte {
            // ASCII `'-'`
            b'-' => sign = -1,
            // ASCII digits `'0'` through `'9'`
            b'0'..=b'9' => result = i64::from(first_byte - b'0'),
            _ => {
                return Err(Error::Other("Invalid decimal integer"));
            }
        }
        if !line.has_remaining() && (sign == -1) {
            // A minus sign must be followed by at least one digit.
            return Err(Error::Other("Invalid decimal integer"));
        }

        // Step 4: Accumulate the remaining digits with checked arithmetic
        // Accumulate negative values directly so `i64::MIN` is representable.
        while line.has_remaining() {
            result = result
                .checked_mul(10)
                .ok_or(Error::Other("Integer overflow"))?;
            let byte = line.get_u8();
            match byte {
                b'0'..=b'9' => {
                    result = result
                        .checked_add(sign * i64::from(byte - b'0'))
                        .ok_or(Error::Other("Integer overflow"))?;
                }
                _ => {
                    return Err(Error::Other("Invalid decimal integer"));
                }
            }
        }
        Ok(result)
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
        // Complete line
        let valid_line = &b"hello world\r\n"[..];
        let mut valid_cursor = Cursor::new(valid_line);
        let valid_bytes = Frame::get_line(&mut valid_cursor);
        assert_eq!(valid_bytes.unwrap(), b"hello world");
        assert_eq!(valid_cursor.position(), 13);

        // Trailing input remains unread
        let superfluous_line = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_line);
        let superfluous_bytes = Frame::get_line(&mut superfluous_cursor);
        assert_eq!(superfluous_bytes.unwrap(), b"2");
        assert_eq!(superfluous_cursor.position(), 3);

        // Missing CRLF terminator
        let inadequate_line = &b"Lorem ipsum"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_line);
        let inadequate_bytes = Frame::get_line(&mut inadequate_cursor);
        assert!(matches!(inadequate_bytes, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Bare carriage return not followed by line feed
        let bare_cr = &b"dolor \rsit amet"[..];
        let mut bare_cr_cursor = Cursor::new(bare_cr);
        let bare_cr_result = Frame::get_line(&mut bare_cr_cursor);
        assert!(matches!(bare_cr_result, Err(Error::Other(_))));
        assert_eq!(bare_cr_cursor.position(), 0);

        // Bare line feed not after carriage return
        let bare_lf = &b"consectetur \nadipiscing elit\r\n"[..];
        let mut bare_lf_cursor = Cursor::new(bare_lf);
        let bare_lf_result = Frame::get_line(&mut bare_lf_cursor);
        assert!(matches!(bare_lf_result, Err(Error::Other(_))));
        assert_eq!(bare_lf_cursor.position(), 0);
    }

    #[test]
    fn test_frame_get_decimal_valid() {
        // Positive integer
        let valid_pos = &b"42\r\n"[..];
        let mut valid_pos_cursor = Cursor::new(valid_pos);
        let valid_pos_int = Frame::get_decimal(&mut valid_pos_cursor);
        assert_eq!(valid_pos_int.unwrap(), 42i64);
        assert_eq!(valid_pos_cursor.position(), 4);

        // Negative integer
        let valid_neg = &b"-137\r\n"[..];
        let mut valid_neg_cursor = Cursor::new(valid_neg);
        let valid_neg_int = Frame::get_decimal(&mut valid_neg_cursor);
        assert_eq!(valid_neg_int.unwrap(), -137i64);
        assert_eq!(valid_neg_cursor.position(), 6);

        // Single-digit integer
        let valid_sig = &b"9\r\n"[..];
        let mut valid_sig_cursor = Cursor::new(valid_sig);
        let valid_sig_int = Frame::get_decimal(&mut valid_sig_cursor);
        assert_eq!(valid_sig_int.unwrap(), 9i64);
        assert_eq!(valid_sig_cursor.position(), 3);

        // Trailing input remains unread
        let superfluous_num = &b"2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut superfluous_cursor = Cursor::new(superfluous_num);
        let superfluous_int = Frame::get_decimal(&mut superfluous_cursor);
        assert_eq!(superfluous_int.unwrap(), 2i64);
        assert_eq!(superfluous_cursor.position(), 3);
    }

    #[test]
    fn test_frame_get_decimal_errors() {
        // Missing CRLF terminator
        let inadequate_num = &b"299792458"[..];
        let mut inadequate_cursor = Cursor::new(inadequate_num);
        let inadequate_int = Frame::get_decimal(&mut inadequate_cursor);
        assert!(matches!(inadequate_int, Err(Error::Incomplete)));
        assert_eq!(inadequate_cursor.position(), 0);

        // Nonnumeric contents
        let non_num = &b"No. 1729\r\n"[..];
        let mut non_cursor = Cursor::new(non_num);
        let non_int = Frame::get_decimal(&mut non_cursor);
        assert!(matches!(non_int, Err(Error::Other(_))));
        assert_eq!(non_cursor.position(), 10);

        // Minus sign without digits
        let only_min = &b"-\r\n"[..];
        let mut min_cursor = Cursor::new(only_min);
        let min_int = Frame::get_decimal(&mut min_cursor);
        assert!(matches!(min_int, Err(Error::Other(_))));
        assert_eq!(min_cursor.position(), 3);

        // Minus sign after digits
        let wrong_min = &b"42-137\r\n"[..];
        let mut wrong_cursor = Cursor::new(wrong_min);
        let wrong_int = Frame::get_decimal(&mut wrong_cursor);
        assert!(matches!(wrong_int, Err(Error::Other(_))));
        assert_eq!(wrong_cursor.position(), 8);
    }

    #[test]
    fn test_frame_get_decimal_limits() {
        // Maximum `i64` value
        let i64max = &b"9223372036854775807\r\n"[..];
        let mut i64max_cursor = Cursor::new(i64max);
        let i64max_int = Frame::get_decimal(&mut i64max_cursor);
        assert_eq!(i64max_int.unwrap(), i64::MAX);
        assert_eq!(i64max_cursor.position(), 21);

        // One above `i64::MAX`
        let i64max_p1 = &b"9223372036854775808\r\n"[..];
        let mut i64max_p1_cursor = Cursor::new(i64max_p1);
        let i64max_p1_int = Frame::get_decimal(&mut i64max_p1_cursor);
        assert!(matches!(i64max_p1_int, Err(Error::Other(_))));
        assert_eq!(i64max_p1_cursor.position(), 21);

        // Minimum `i64` value
        let i64min = &b"-9223372036854775808\r\n"[..];
        let mut i64min_cursor = Cursor::new(i64min);
        let i64min_int = Frame::get_decimal(&mut i64min_cursor);
        assert_eq!(i64min_int.unwrap(), i64::MIN);
        assert_eq!(i64min_cursor.position(), 22);

        // One below `i64::MIN`
        let i64min_m1 = &b"-9223372036854775809\r\n"[..];
        let mut i64min_m1_cursor = Cursor::new(i64min_m1);
        let i64min_m1_int = Frame::get_decimal(&mut i64min_m1_cursor);
        assert!(matches!(i64min_m1_int, Err(Error::Other(_))));
        assert_eq!(i64min_m1_cursor.position(), 22);

        // Multiplication overflow (`i64::MAX * 10`)
        let i64max_x10 = &b"92233720368547758070\r\n"[..];
        let mut i64max_x10_cursor = Cursor::new(i64max_x10);
        let i64max_x10_int = Frame::get_decimal(&mut i64max_x10_cursor);
        assert!(matches!(i64max_x10_int, Err(Error::Other(_))));
        assert_eq!(i64max_x10_cursor.position(), 22);
    }

    #[test]
    fn test_frame_check_line_frames() {
        // Simple string
        let valid_simple = &b"+Hello, world!\r\n"[..];
        let mut simple_cursor = Cursor::new(valid_simple);
        let simple_result = Frame::check(&mut simple_cursor);
        assert!(simple_result.is_ok());
        assert_eq!(simple_cursor.position(), 16);

        // Error frame
        let valid_error = &b"-Error 404 Not Found\r\n"[..];
        let mut error_cursor = Cursor::new(valid_error);
        let error_result = Frame::check(&mut error_cursor);
        assert!(error_result.is_ok());
        assert_eq!(error_cursor.position(), 22);

        // Integer frame
        let valid_integer = &b":42\r\n"[..];
        let mut integer_cursor = Cursor::new(valid_integer);
        let integer_result = Frame::check(&mut integer_cursor);
        assert!(integer_result.is_ok());
        assert_eq!(integer_cursor.position(), 5);
    }

    #[test]
    fn test_frame_check_bulk() {
        // Bulk string
        let valid_bulk = &b"$6\r\nfoobar\r\n"[..];
        let mut bulk_cursor = Cursor::new(valid_bulk);
        let bulk_result = Frame::check(&mut bulk_cursor);
        assert!(bulk_result.is_ok());
        assert_eq!(bulk_cursor.position(), 12);

        // Empty bulk string
        let valid_emptybulk = &b"$0\r\n\r\n"[..];
        let mut emptybulk_cursor = Cursor::new(valid_emptybulk);
        let emptybulk_result = Frame::check(&mut emptybulk_cursor);
        assert!(emptybulk_result.is_ok());
        assert_eq!(emptybulk_cursor.position(), 6);

        // Null bulk string
        let valid_nullbulk = &b"$-1\r\n"[..];
        let mut nullbulk_cursor = Cursor::new(valid_nullbulk);
        let nullbulk_result = Frame::check(&mut nullbulk_cursor);
        assert!(nullbulk_result.is_ok());
        assert_eq!(nullbulk_cursor.position(), 5);

        // Incomplete bulk payload
        let inadequate_bulk = &b"$6\r\nfoo"[..];
        let mut inadequate_bulk_cursor = Cursor::new(inadequate_bulk);
        let inadequate_bulk_result = Frame::check(&mut inadequate_bulk_cursor);
        assert!(matches!(inadequate_bulk_result, Err(Error::Incomplete)));
        // The cursor is at the payload after reading the length field.
        assert_eq!(inadequate_bulk_cursor.position(), 4);

        // Invalid negative bulk length
        let wrong_bulklen = &b"$-42\r\n"[..];
        let mut wrong_bulklen_cursor = Cursor::new(wrong_bulklen);
        let wrong_bulklen_result = Frame::check(&mut wrong_bulklen_cursor);
        assert!(matches!(wrong_bulklen_result, Err(Error::Other(_))));
        assert_eq!(wrong_bulklen_cursor.position(), 6);

        // Invalid bulk terminator
        let wrong_bulk = &b"$6\r\nfoobar\r3"[..];
        let mut wrong_bulk_cursor = Cursor::new(wrong_bulk);
        let wrong_bulk_result = Frame::check(&mut wrong_bulk_cursor);
        assert!(matches!(wrong_bulk_result, Err(Error::Other(_))));
        assert_eq!(wrong_bulk_cursor.position(), 12);

        // Incomplete bulk terminator
        let bulk_terminator = &b"$6\r\nfoobar\r"[..];
        let mut bulk_terminator_cursor = Cursor::new(bulk_terminator);
        let bulk_terminator_result = Frame::check(&mut bulk_terminator_cursor);
        assert!(matches!(bulk_terminator_result, Err(Error::Incomplete)));
        assert_eq!(bulk_terminator_cursor.position(), 4);

        // Bulk length at `i64::MAX`
        let i64max_length = &b"$9223372036854775807\r\n"[..];
        let mut i64max_length_cursor = Cursor::new(i64max_length);
        let i64max_length_result = Frame::check(&mut i64max_length_cursor);
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(i64max_length_result, Err(Error::Incomplete)));

        #[cfg(target_pointer_width = "32")]
        assert!(matches!(
            i64max_length_result,
            Err(Error::Other("Bulk string length overflow"))
        ));
    }

    #[test]
    fn test_frame_check_array() {
        // Ordinary array
        let valid_array = &b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..];
        let mut array_cursor = Cursor::new(valid_array);
        let array_result = Frame::check(&mut array_cursor);
        assert!(array_result.is_ok());
        assert_eq!(array_cursor.position(), 22);

        // Nested array
        let valid_nestarray = &b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"[..];
        let mut nestarray_cursor = Cursor::new(valid_nestarray);
        let nestarray_result = Frame::check(&mut nestarray_cursor);
        assert!(nestarray_result.is_ok());
        assert_eq!(nestarray_cursor.position(), 36);

        // Empty array
        let valid_emptyarray = &b"*0\r\n"[..];
        let mut emptyarray_cursor = Cursor::new(valid_emptyarray);
        let emptyarray_result = Frame::check(&mut emptyarray_cursor);
        assert!(emptyarray_result.is_ok());
        assert_eq!(emptyarray_cursor.position(), 4);

        // Null array
        let valid_nullarray = &b"*-1\r\n"[..];
        let mut nullarray_cursor = Cursor::new(valid_nullarray);
        let nullarray_result = Frame::check(&mut nullarray_cursor);
        assert!(nullarray_result.is_ok());
        assert_eq!(nullarray_cursor.position(), 5);

        // Incomplete array
        let cutoff_array = &b"*2\r\n$3\r\nfoo\r\n"[..];
        let mut cutoff_cursor = Cursor::new(cutoff_array);
        let cutoff_result = Frame::check(&mut cutoff_cursor);
        assert!(matches!(cutoff_result, Err(Error::Incomplete)));
        assert_eq!(cutoff_cursor.position(), 13);

        // Invalid negative array length
        let wrong_arraysize = &b"*-137\r\n"[..];
        let mut wrong_arraysize_cursor = Cursor::new(wrong_arraysize);
        let wrong_arraysize_result = Frame::check(&mut wrong_arraysize_cursor);
        assert!(matches!(wrong_arraysize_result, Err(Error::Other(_))));
        assert_eq!(wrong_arraysize_cursor.position(), 7);
    }

    #[test]
    fn test_frame_check_input_errors() {
        // Empty input
        let empty_data = &b""[..];
        let mut empty_cursor = Cursor::new(empty_data);
        let empty_result = Frame::check(&mut empty_cursor);
        assert!(matches!(empty_result, Err(Error::Incomplete)));
        assert_eq!(empty_cursor.position(), 0);

        // Unknown frame marker
        let wrong_1stbyte = &b"&Lorem ipsum\r\n"[..];
        let mut wrong_1stbyte_cursor = Cursor::new(wrong_1stbyte);
        let wrong_1stbyte_result = Frame::check(&mut wrong_1stbyte_cursor);
        assert!(matches!(wrong_1stbyte_result, Err(Error::Other(_))));
        assert_eq!(wrong_1stbyte_cursor.position(), 1);

        // Bare carriage return in simple string
        let bare_cr_simple = &b"+dolor \rsit amet"[..];
        let mut bare_cr_simple_cursor = Cursor::new(bare_cr_simple);
        let bare_cr_simple_result = Frame::check(&mut bare_cr_simple_cursor);
        assert!(matches!(bare_cr_simple_result, Err(Error::Other(_))));
        assert_eq!(bare_cr_simple_cursor.position(), 1);

        // Bare carriage return in error
        let bare_cr_error = &b"-dolor \rsit amet"[..];
        let mut bare_cr_error_cursor = Cursor::new(bare_cr_error);
        let bare_cr_error_result = Frame::check(&mut bare_cr_error_cursor);
        assert!(matches!(bare_cr_error_result, Err(Error::Other(_))));
        assert_eq!(bare_cr_error_cursor.position(), 1);

        // Bare line feed in simple string
        let bare_lf_simple = &b"+consectetur \nadipiscing elit\r\n"[..];
        let mut bare_lf_simple_cursor = Cursor::new(bare_lf_simple);
        let bare_lf_simple_result = Frame::check(&mut bare_lf_simple_cursor);
        assert!(matches!(bare_lf_simple_result, Err(Error::Other(_))));
        assert_eq!(bare_lf_simple_cursor.position(), 1);

        // Bare line feed in error
        let bare_lf_error = &b"-consectetur \nadipiscing elit\r\n"[..];
        let mut bare_lf_error_cursor = Cursor::new(bare_lf_error);
        let bare_lf_error_result = Frame::check(&mut bare_lf_error_cursor);
        assert!(matches!(bare_lf_error_result, Err(Error::Other(_))));
        assert_eq!(bare_lf_error_cursor.position(), 1);
    }

    #[test]
    fn test_frame_check_oversized_array() {
        // Array length at the exclusive limit
        let size = config::ARRAY_LENGTH_LIMIT_EXCLUSIVE;
        let oversized_array = format!("*{size}\r\n");
        let mut oversized_array_cursor = Cursor::new(oversized_array.as_bytes());
        let oversized_array_result = Frame::check(&mut oversized_array_cursor);
        assert!(matches!(oversized_array_result, Err(Error::Other(_))));
    }

    #[test]
    fn test_frame_check_overnested_array() {
        // Array nesting beyond the supported depth
        let subframe = "*1\r\n".repeat(33);
        let overnested_array = format!("{subframe}:0\r\n");
        let mut overnested_array_cursor = Cursor::new(overnested_array.as_bytes());
        let overnested_array_result = Frame::check(&mut overnested_array_cursor);
        assert!(matches!(overnested_array_result, Err(Error::Other(_))));
    }

    #[test]
    fn test_frame_parse_line_frames() {
        // Simple string
        let valid_simple = &b"+Hello, World!\r\n"[..];
        let mut simple_cursor = Cursor::new(valid_simple);
        let simple_frame = Frame::parse(&mut simple_cursor);
        assert_eq!(
            simple_frame.unwrap(),
            Frame::Simple("Hello, World!".to_string())
        );
        assert_eq!(simple_cursor.position(), 16);

        // Error frame
        let valid_error = &b"-Error 404 Not Found\r\n"[..];
        let mut error_cursor = Cursor::new(valid_error);
        let error_frame = Frame::parse(&mut error_cursor);
        assert_eq!(
            error_frame.unwrap(),
            Frame::Error("Error 404 Not Found".to_string())
        );
        assert_eq!(error_cursor.position(), 22);

        // Integer frame
        let valid_integer = &b":42\r\n"[..];
        let mut integer_cursor = Cursor::new(valid_integer);
        let integer_frame = Frame::parse(&mut integer_cursor);
        assert_eq!(integer_frame.unwrap(), Frame::Integer(42i64));
        assert_eq!(integer_cursor.position(), 5);

        // Nonnumeric integer contents
        let non_num_int = &b":abc\r\n"[..];
        let mut non_num_int_cursor = Cursor::new(non_num_int);
        let non_num_int_frame = Frame::parse(&mut non_num_int_cursor);
        assert!(matches!(non_num_int_frame, Err(Error::Other(_))));
    }

    #[test]
    fn test_frame_parse_bulk() {
        // Bulk string
        let valid_bulk = &b"$6\r\nfoobar\r\n"[..];
        let mut bulk_cursor = Cursor::new(valid_bulk);
        let bulk_frame = Frame::parse(&mut bulk_cursor);
        assert_eq!(bulk_frame.unwrap(), Frame::Bulk("foobar".as_bytes().into()));
        assert_eq!(bulk_cursor.position(), 12);

        // Empty bulk string
        let valid_emptybulk = &b"$0\r\n\r\n"[..];
        let mut emptybulk_cursor = Cursor::new(valid_emptybulk);
        let emptybulk_frame = Frame::parse(&mut emptybulk_cursor);
        assert_eq!(emptybulk_frame.unwrap(), Frame::Bulk("".as_bytes().into()));
        assert_eq!(emptybulk_cursor.position(), 6);

        // Null bulk string
        let valid_nullbulk = &b"$-1\r\n"[..];
        let mut nullbulk_cursor = Cursor::new(valid_nullbulk);
        let nullbulk_frame = Frame::parse(&mut nullbulk_cursor);
        assert_eq!(nullbulk_frame.unwrap(), Frame::Null);
        assert_eq!(nullbulk_cursor.position(), 5);

        // Incomplete bulk payload
        let truncated_bulk = &b"$6\r\nfoo"[..];
        let mut truncated_bulk_cursor = Cursor::new(truncated_bulk);
        let truncated_bulk_frame = Frame::parse(&mut truncated_bulk_cursor);
        assert!(matches!(truncated_bulk_frame, Err(Error::Incomplete)));

        // Invalid bulk terminator
        let wrong_end_bulk = &b"$6\r\nfoobar@@"[..];
        let mut wrong_end_bulk_cursor = Cursor::new(wrong_end_bulk);
        let wrong_end_bulk_frame = Frame::parse(&mut wrong_end_bulk_cursor);
        assert!(matches!(wrong_end_bulk_frame, Err(Error::Other(_))));
    }

    #[test]
    fn test_frame_parse_array() {
        // Ordinary array
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

        // Nested array
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

        // Empty array
        let valid_emptyarray = &b"*0\r\n"[..];
        let mut emptyarray_cursor = Cursor::new(valid_emptyarray);
        let emptyarray_frame = Frame::parse(&mut emptyarray_cursor);
        assert_eq!(emptyarray_frame.unwrap(), Frame::Array(vec![]));
        assert_eq!(emptyarray_cursor.position(), 4);

        // Null array
        let valid_nullarray = &b"*-1\r\n"[..];
        let mut nullarray_cursor = Cursor::new(valid_nullarray);
        let nullarray_frame = Frame::parse(&mut nullarray_cursor);
        assert_eq!(nullarray_frame.unwrap(), Frame::Null);
        assert_eq!(nullarray_cursor.position(), 5);
    }

    #[test]
    fn test_frame_parse_input_errors() {
        // Empty input
        let empty_input = &b""[..];
        let mut empty_input_cursor = Cursor::new(empty_input);
        let empty_input_frame = Frame::parse(&mut empty_input_cursor);
        assert!(matches!(empty_input_frame, Err(Error::Incomplete)));

        // Unknown frame marker
        let wrong_1stbyte = &b"&Lorem ipsum\r\n"[..];
        let mut wrong_1stbyte_cursor = Cursor::new(wrong_1stbyte);
        let wrong_1stbyte_frame = Frame::parse(&mut wrong_1stbyte_cursor);
        assert!(matches!(wrong_1stbyte_frame, Err(Error::Other(_))));
        // Reading the frame marker advances the cursor by one byte.
        assert_eq!(wrong_1stbyte_cursor.position(), 1);

        // Bare carriage return in simple string
        let bare_cr_simple = &b"+dolor \rsit amet"[..];
        let mut bare_cr_simple_cursor = Cursor::new(bare_cr_simple);
        let bare_cr_simple_result = Frame::parse(&mut bare_cr_simple_cursor);
        assert!(matches!(bare_cr_simple_result, Err(Error::Other(_))));
        assert_eq!(bare_cr_simple_cursor.position(), 1);

        // Bare carriage return in error
        let bare_cr_error = &b"-dolor \rsit amet"[..];
        let mut bare_cr_error_cursor = Cursor::new(bare_cr_error);
        let bare_cr_error_result = Frame::parse(&mut bare_cr_error_cursor);
        assert!(matches!(bare_cr_error_result, Err(Error::Other(_))));
        assert_eq!(bare_cr_error_cursor.position(), 1);

        // Bare line feed in simple string
        let bare_lf_simple = &b"+consectetur \nadipiscing elit\r\n"[..];
        let mut bare_lf_simple_cursor = Cursor::new(bare_lf_simple);
        let bare_lf_simple_result = Frame::parse(&mut bare_lf_simple_cursor);
        assert!(matches!(bare_lf_simple_result, Err(Error::Other(_))));
        assert_eq!(bare_lf_simple_cursor.position(), 1);

        // Bare line feed in error
        let bare_lf_error = &b"-consectetur \nadipiscing elit\r\n"[..];
        let mut bare_lf_error_cursor = Cursor::new(bare_lf_error);
        let bare_lf_error_result = Frame::parse(&mut bare_lf_error_cursor);
        assert!(matches!(bare_lf_error_result, Err(Error::Other(_))));
        assert_eq!(bare_lf_error_cursor.position(), 1);
    }

    #[test]
    fn test_frame_parse_nonzero_cursor() {
        // Parse the second frame, leaving the third frame unread.
        let long_inputs = "$3\r\nSET\r\n$5\r\nAlpha\r\n$3\r\n137\r\n";
        let mut long_inputs_cursor = Cursor::new(long_inputs.as_bytes());
        long_inputs_cursor.set_position(9);
        let long_inputs_frame = Frame::parse(&mut long_inputs_cursor);
        assert_eq!(
            long_inputs_frame.unwrap(),
            Frame::Bulk("Alpha".as_bytes().into())
        );
        assert_eq!(long_inputs_cursor.position(), 20);
    }

    #[test]
    fn test_frame_parse_maxsized_array() {
        // Array at the maximum supported length
        let size = usize::try_from(config::ARRAY_LENGTH_LIMIT_EXCLUSIVE - 1).unwrap();
        let subframe = ":0\r\n".repeat(size);
        let maxsized_array = format!("*{size}\r\n{subframe}");
        let mut maxsized_array_cursor = Cursor::new(maxsized_array.as_bytes());
        let maxsized_array_result = Frame::parse(&mut maxsized_array_cursor);
        assert_eq!(
            maxsized_array_result.unwrap(),
            // Build the expected elements without requiring `Frame: Clone`.
            Frame::Array((0..size).map(|_| Frame::Integer(0)).collect())
        );
        // Count the array header and all encoded child frames.
        let position = u64::try_from(
            1 + usize::try_from(size.checked_ilog10().unwrap_or(0) + 1).unwrap() + 4 * size + 2,
        )
        .unwrap();
        assert_eq!(maxsized_array_cursor.position(), position);
    }

    #[test]
    fn test_frame_parse_maxnested_array() {
        // Array at the maximum supported nesting depth
        let level = usize::try_from(config::MAX_ARRAY_DEPTH).unwrap();
        let subframe = "*1\r\n".repeat(level);
        let maxnested_array = format!("{subframe}:0\r\n");
        let mut maxnested_array_cursor = Cursor::new(maxnested_array.as_bytes());
        let maxnested_array_result = Frame::parse(&mut maxnested_array_cursor);
        // Build the expected array from the innermost value outward.
        let mut expected = Frame::Integer(0);
        for _ in 0..level {
            expected = Frame::Array(vec![expected]);
        }
        assert_eq!(maxnested_array_result.unwrap(), expected);
        let position = u64::try_from(4 * level + 4).unwrap();
        assert_eq!(maxnested_array_cursor.position(), position);
    }
}
