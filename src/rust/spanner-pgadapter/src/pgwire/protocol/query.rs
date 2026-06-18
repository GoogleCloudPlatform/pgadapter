// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::pgwire::protocol::{Decode, Encode, read_string, write_string};
use bytes::{BufMut, BytesMut};

pub(crate) const QUERY_IDENTIFIER: u8 = b'Q';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Query {
    pub(crate) sql: String,
}

impl<'a> Decode<'a> for Query {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let sql = read_string(source)?;
        Ok(Query {
            sql: sql.to_string(),
        })
    }
}

pub(crate) const COMMAND_COMPLETE_IDENTIFIER: u8 = b'C';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CommandComplete<'a> {
    pub(crate) tag: &'a str,
}

impl<'a> Encode for CommandComplete<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let payload_len = 4 + self.tag.len() + 1;

        destination.reserve(1 + payload_len);
        destination.put_u8(COMMAND_COMPLETE_IDENTIFIER);
        destination.put_i32(payload_len as i32);

        write_string(destination, self.tag);

        Ok(())
    }
}

pub(crate) const EMPTY_QUERY_RESPONSE_IDENTIFIER: u8 = b'I';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct EmptyQueryResponse;

impl Encode for EmptyQueryResponse {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.reserve(5);
        destination.put_u8(EMPTY_QUERY_RESPONSE_IDENTIFIER);
        destination.put_i32(4);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_decode() {
        let mut data = BytesMut::new();
        write_string(&mut data, "SELECT * FROM users;");

        let payload = data.freeze();
        let mut source = &payload[..];
        let query = Query::decode(&mut source).expect("failed to decode Query");

        assert_eq!(query.sql, "SELECT * FROM users;");
        assert!(source.is_empty());
    }

    #[test]
    fn test_command_complete_encode() {
        let mut buffer = BytesMut::new();
        let command_complete = CommandComplete { tag: "SELECT 42" };
        command_complete
            .encode(&mut buffer)
            .expect("failed to encode CommandComplete");

        let expected_payload = b"SELECT 42\0";
        let expected_len = (4 + expected_payload.len()) as i32;

        let mut expected = BytesMut::new();
        expected.put_u8(b'C');
        expected.put_i32(expected_len);
        expected.put_slice(expected_payload);

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_query_decode_malformed() {
        let payload = b"SELECT * FROM users;"; // missing null terminator
        let mut source = &payload[..];
        let result = Query::decode(&mut source);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_query_response_encode() {
        let mut buffer = BytesMut::new();
        EmptyQueryResponse
            .encode(&mut buffer)
            .expect("failed to encode EmptyQueryResponse");
        assert_eq!(buffer.to_vec(), vec![b'I', 0, 0, 0, 4]);
    }
}
