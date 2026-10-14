use crate::database::Database;
use crate::frame::Frame;
use crate::parse::{Error, Parse};
use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub(crate) fn from_parse(parser: &mut Parse) -> Result<Self, Error> {
        // Step 1: Extract the key
        let key = parser.next_string()?;

        // Step 2: Check for extra arguments
        parser.finish()?;

        Ok(Self { key })
    }
}

#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl Set {
    pub(crate) fn from_parse(parser: &mut Parse) -> Result<Self, Error> {
        // Step 1: Extract the key and value
        let key = parser.next_string()?;
        let value = parser.next_bytes()?;

        // Step 2: Check for extra arguments
        parser.finish()?;

        Ok(Self { key, value })
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
}

impl Command {
    /// Parses a `GET` or `SET` command from an array frame.
    ///
    /// Command names are matched case-insensitively using ASCII rules.
    ///
    /// # Errors
    /// Returns `Error::EndOfStream` if the command name or a required argument
    /// is missing.
    /// Returns `Error::Other` if the input is not an array, the command is
    /// unsupported, an argument has an invalid type, the command name or key
    /// is not valid UTF-8, or extra arguments remain.
    pub fn from_frame(frame: Frame) -> Result<Self, Error> {
        // Step 1: Create an argument parser from the frame
        let mut parser = Parse::from_frame(frame)?;

        // Step 2: Extract the command name
        let command_name = parser.next_string()?;

        // Step 3: Parse the arguments for the selected command
        if command_name.eq_ignore_ascii_case("GET") {
            // Existing GET branch expression.
            return Ok(Self::Get(Get::from_parse(&mut parser)?));
        }

        if command_name.eq_ignore_ascii_case("SET") {
            // Existing SET branch expression.
            return Ok(Self::Set(Set::from_parse(&mut parser)?));
        }

        Err(Error::Other("Unsupported command; expected GET or SET"))
    }

    pub fn apply(self, database: &Database) -> Frame {
        // Step 1: Select the command to execute
        match self {
            // GET command
            Command::Get(command) => match database.get(command.key.as_str()) {
                // Existing key
                Some(bytes) => Frame::Bulk(bytes),
                // Missing key
                None => Frame::Null,
            },

            // SET command
            Command::Set(command) => {
                // Step 2: Store the value and return an acknowledgement
                database.set(command.key, command.value);
                Frame::Simple(String::from("OK"))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod tests {
    use super::*;

    #[test]
    fn test_get_from_parse() {
        // Valid command
        let mut valid_get_parser = Parse::new_test(vec![Frame::Bulk(Bytes::from("Answer"))]);
        let valid_get_command = Get::from_parse(&mut valid_get_parser).unwrap();
        assert_eq!(
            valid_get_command,
            Get {
                key: String::from("Answer")
            }
        );

        // Extra argument
        let mut extra_argument_parser = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Error(String::from("me!")),
        ]);
        let extra_argument_result = Get::from_parse(&mut extra_argument_parser);
        assert!(matches!(extra_argument_result, Err(Error::Other(_))));

        // The tests in `parse.rs` cover argument-iterator exhaustion.
    }

    #[test]
    fn test_set_from_parse() {
        // Valid command
        let mut valid_set_parser = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
        ]);
        let valid_set_command = Set::from_parse(&mut valid_set_parser).unwrap();
        assert_eq!(
            valid_set_command,
            Set {
                key: String::from("Answer"),
                value: Bytes::from("42"),
            }
        );

        // Extra argument
        let mut extra_argument_parser = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
            Frame::Null,
        ]);
        let extra_argument_result = Set::from_parse(&mut extra_argument_parser);
        assert!(matches!(extra_argument_result, Err(Error::Other(_))));

        // The tests in `parse.rs` cover argument-iterator exhaustion.
    }

    #[test]
    fn test_command_from_frame() {
        // Valid GET command with a mixed-case name
        let valid_get_frame = Frame::Array(vec![
            Frame::Simple(String::from("Get")),
            Frame::Bulk(Bytes::from("Answer")),
        ]);

        let valid_get_command = Command::from_frame(valid_get_frame).unwrap();
        assert_eq!(
            valid_get_command,
            Command::Get(Get {
                key: String::from("Answer")
            })
        );

        // Valid SET command with a mixed-case name
        let valid_set_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("sET")),
            Frame::Simple(String::from("Answer")),
            Frame::Simple(String::from("42")),
        ]);

        let valid_set_command = Command::from_frame(valid_set_frame).unwrap();
        assert_eq!(
            valid_set_command,
            Command::Set(Set {
                key: String::from("Answer"),
                value: Bytes::from("42")
            })
        );

        // Unsupported command
        let unsupported_frame = Frame::Array(vec![
            Frame::Simple(String::from("sudo")),
            Frame::Bulk(Bytes::from("rm -rf /")),
        ]);

        let unsupported_result = Command::from_frame(unsupported_frame);
        assert!(matches!(unsupported_result, Err(Error::Other(_))));

        // Unsupported letter
        let unsupported_letter = Frame::Array(vec![
            Frame::Bulk(Bytes::from("\u{017F}ET")),
            Frame::Simple(String::from("Answer")),
            Frame::Simple(String::from("42")),
        ]);

        let unsupported_result = Command::from_frame(unsupported_letter);
        assert!(matches!(unsupported_result, Err(Error::Other(_))));
    }

    #[test]
    fn test_command_apply() {
        let database = Database::new();

        // Store an entry
        let set_command = Command::Set(Set {
            key: String::from("Answer"),
            value: Bytes::from("42"),
        });
        let set_response = set_command.apply(&database);
        assert_eq!(set_response, Frame::Simple(String::from("OK")));

        // Retrieve an existing value
        let existing_key_command = Command::Get(Get {
            key: String::from("Answer"),
        });
        let existing_key_response = existing_key_command.apply(&database);
        assert_eq!(existing_key_response, Frame::Bulk(Bytes::from("42")));

        // Look up a missing key
        let missing_key_command = Command::Get(Get {
            key: String::from("Alpha"),
        });
        let missing_key_response = missing_key_command.apply(&database);
        assert_eq!(missing_key_response, Frame::Null);
    }
}
