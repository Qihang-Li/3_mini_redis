use crate::database::Database;
use crate::frame::Frame;
use crate::parse::{Error, Parse};
use bytes::Bytes;

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: String,
}

impl Get {
    pub(crate) fn from_parse(parse: &mut Parse) -> Result<Self, Error> {
        // Step 1: Extract the key
        let key = parse.next_string()?;

        // Step 2: Check for extra arguments
        parse.finish()?;

        Ok(Self { key })
    }
}

#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: String,
    pub value: Bytes,
}

impl Set {
    pub(crate) fn from_parse(parse: &mut Parse) -> Result<Self, Error> {
        // Step 1: Extract the key and value
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;

        // Step 2: Check for extra arguments
        parse.finish()?;

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
    /// Command names are case-insensitive. Validates the required arguments
    /// and rejects extra arguments.
    ///
    /// # Errors
    /// Returns `Error::EndOfStream` if the command name or a required argument
    /// is missing.
    /// Returns `Error::Other` if the input is not an array, the command is
    /// unsupported, an argument has an invalid type, the command name or key
    /// is not valid UTF-8, or extra arguments remain.
    pub fn from_frame(frame: Frame) -> Result<Self, Error> {
        // Step 1: Create an argument parser from the frame
        let mut parse = Parse::from_frame(frame)?;

        // Step 2: Extract and normalize the command name
        let command = parse.next_string()?.to_uppercase();

        // Step 3: Parse the arguments for the selected command
        match command.as_str() {
            // GET command
            "GET" => Ok(Self::Get(Get::from_parse(&mut parse)?)),
            // SET command
            "SET" => Ok(Self::Set(Set::from_parse(&mut parse)?)),
            // Unsupported command
            _ => Err(Error::Other(
                "Wrong message: Unsupported command, GET or SET expected",
            )),
        }
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
        let mut valid_get_parse = Parse::new_test(vec![Frame::Bulk(Bytes::from("Answer"))]);
        let valid_get_command = Get::from_parse(&mut valid_get_parse).unwrap();
        assert_eq!(
            valid_get_command,
            Get {
                key: String::from("Answer")
            }
        );

        // Extra argument
        let mut superfluous_get_parse = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Error(String::from("me!")),
        ]);
        let superfluous_get_command = Get::from_parse(&mut superfluous_get_parse);
        assert!(matches!(superfluous_get_command, Err(Error::Other(_))));

        // The tests in `parse.rs` cover argument-iterator exhaustion.
    }

    #[test]
    fn test_set_from_parse() {
        // Valid command
        let mut valid_set_parse = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
        ]);
        let valid_set_command = Set::from_parse(&mut valid_set_parse).unwrap();
        assert_eq!(
            valid_set_command,
            Set {
                key: String::from("Answer"),
                value: Bytes::from("42"),
            }
        );

        // Extra argument
        let mut superfluous_set_parse = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
            Frame::Null,
        ]);
        let superfluous_set_command = Set::from_parse(&mut superfluous_set_parse);
        assert!(matches!(superfluous_set_command, Err(Error::Other(_))));

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
        let invalid_frame = Frame::Array(vec![
            Frame::Simple(String::from("sudo")),
            Frame::Bulk(Bytes::from("rm -rf /")),
        ]);

        let invalid_command = Command::from_frame(invalid_frame);
        assert!(matches!(invalid_command, Err(Error::Other(_))));
    }

    #[test]
    fn test_command_apply() {
        let test_db = Database::new();

        // Store an entry
        let set_command = Command::Set(Set {
            key: String::from("Answer"),
            value: Bytes::from("42"),
        });
        let set_resp_frame = set_command.apply(&test_db);
        assert_eq!(set_resp_frame, Frame::Simple(String::from("OK")));

        // Retrieve an existing value
        let get_valid_command = Command::Get(Get {
            key: String::from("Answer"),
        });
        let get_valid_resp_frame = get_valid_command.apply(&test_db);
        assert_eq!(get_valid_resp_frame, Frame::Bulk(Bytes::from("42")));

        // Look up a missing key
        let get_invalid_command = Command::Get(Get {
            key: String::from("Alpha"),
        });
        let get_invalid_resp_frame = get_invalid_command.apply(&test_db);
        assert_eq!(get_invalid_resp_frame, Frame::Null);
    }
}
