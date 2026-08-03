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
        // Step 1: extract the key
        let key = parse.next_string()?;

        // Step 2: verify the end of the command
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
        // Step 1: extract the key and value
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;

        // Step 2: verify the end of the command
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
    /// Creates a command object of either `Get` or `Set` using a `Frame`
    ///
    /// # Errors
    /// Returns `Error::Other` if the command is something else.
    pub fn from_frame(frame: Frame) -> Result<Self, Error> {
        // Step 1: create a Parse object using the frame
        let mut parse = Parse::from_frame(frame)?;

        // Step 2: extract the first element as the type of command
        let command = parse.next_string()?.to_uppercase();

        // Step 3: match the type of command and return corresponding command
        match command.as_str() {
            // 3.(i) a get command
            "GET" => Ok(Self::Get(Get::from_parse(&mut parse)?)),
            // 3.(ii) a set command
            "SET" => Ok(Self::Set(Set::from_parse(&mut parse)?)),
            // 3.(iii) something else, here being an invalid command
            _ => Err(Error::Other(
                "Wrong message: Unsupported command, GET or SET expected",
            )),
        }
    }

    pub fn apply(self, database: &Database) -> Frame {
        // Step 1: match the command
        match self {
            // 1.(i): a get command
            Command::Get(command) => match database.get(command.key.as_str()) {
                // Step 2: execute the command
                Some(bytes) => Frame::Bulk(bytes),
                None => Frame::Null,
            },

            // 1.(ii): a set command
            Command::Set(command) => {
                // Step 2: execute the command
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
        // Test 1: Valid command
        let mut valid_get_parse = Parse::new_test(vec![Frame::Bulk(Bytes::from("Answer"))]);
        let valid_get_command = Get::from_parse(&mut valid_get_parse).unwrap();
        assert_eq!(
            valid_get_command,
            Get {
                key: String::from("Answer")
            }
        );

        // Test 2: Superfluous command
        let mut superfluous_get_parse = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Error(String::from("me!")),
        ]);
        let superfluous_get_command = Get::from_parse(&mut superfluous_get_parse);
        assert!(matches!(superfluous_get_command, Err(Error::Other(_))));

        // No Test 3: No inadequate command
        // Since it is verified by `Parse::next_bytes()`.
    }

    #[test]
    fn test_set_from_parse() {
        // Test 1: Valid command
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

        // Test 2: Superfluous command
        let mut superfluous_set_parse = Parse::new_test(vec![
            Frame::Bulk(Bytes::from("Answer")),
            Frame::Bulk(Bytes::from("42")),
            Frame::Null,
        ]);
        let superfluous_set_command = Set::from_parse(&mut superfluous_set_parse);
        assert!(matches!(superfluous_set_command, Err(Error::Other(_))));

        // No Test 3: No inadequate command
        // Since it is verified by `Parse::next_bytes()`.
    }

    #[test]
    fn test_command_from_frame() {
        // Test 1: Valid GET command
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

        // Test 2: Valid SET command
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

        // Test 3: Invalid command
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

        // Test 1: insert an entry
        let set_command = Command::Set(Set {
            key: String::from("Answer"),
            value: Bytes::from("42"),
        });
        let set_resp_frame = set_command.apply(&test_db);
        assert_eq!(set_resp_frame, Frame::Simple(String::from("OK")));

        // Test 2: get value of existing entry
        let get_valid_command = Command::Get(Get {
            key: String::from("Answer"),
        });
        let get_valid_resp_frame = get_valid_command.apply(&test_db);
        assert_eq!(get_valid_resp_frame, Frame::Bulk(Bytes::from("42")));

        // Test 3: get value of non-existing entry
        let get_invalid_command = Command::Get(Get {
            key: String::from("Alpha"),
        });
        let get_invalid_resp_frame = get_invalid_command.apply(&test_db);
        assert_eq!(get_invalid_resp_frame, Frame::Null);
    }
}
