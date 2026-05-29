use bytes::Bytes;

pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Null,
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
