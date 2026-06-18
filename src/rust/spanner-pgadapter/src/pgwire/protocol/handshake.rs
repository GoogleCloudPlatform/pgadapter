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

use crate::pgwire::protocol::{Decode, Encode, read_string, read_u32, write_string};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StartupMessage {
    pub(crate) protocol_version: u32,
    pub(crate) parameters: HashMap<String, String>,
}

impl<'a> Decode<'a> for StartupMessage {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let protocol_version = read_u32(source)
            .map_err(|_| anyhow::anyhow!("insufficient bytes for protocol version"))?;

        let major = (protocol_version >> 16) as u16;
        let minor = (protocol_version & 0xffff) as u16;
        if major != 3 || minor > 2 {
            return Err(anyhow::anyhow!(
                "unsupported protocol version: {}.{}",
                major,
                minor
            ));
        }

        let mut parameters = HashMap::new();

        loop {
            if source.is_empty() {
                return Err(anyhow::anyhow!(
                    "startup message parameters not terminated properly"
                ));
            }
            if source[0] == 0 {
                *source = &source[1..]; // consume the terminating 0 byte
                break;
            }
            let key = read_string(source)?.to_string();
            let value = read_string(source)?.to_string();
            parameters.insert(key, value);
        }
        Ok(StartupMessage {
            protocol_version,
            parameters,
        })
    }
}

pub(crate) const AUTHENTICATION_OK_IDENTIFIER: u8 = b'R';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AuthenticationOk;

impl Encode for AuthenticationOk {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(AUTHENTICATION_OK_IDENTIFIER);

        destination.put_i32(8); // length (4-byte length + 4-byte payload)
        destination.put_i32(0); // authentication success code (0)
        Ok(())
    }
}

pub(crate) const PARAMETER_STATUS_IDENTIFIER: u8 = b'S';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParameterStatus<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: &'a str,
}

impl<'a> Encode for ParameterStatus<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let payload_len = 4 + self.name.len() + 1 + self.value.len() + 1;

        destination.reserve(1 + payload_len);
        destination.put_u8(PARAMETER_STATUS_IDENTIFIER);
        destination.put_i32(payload_len as i32);

        write_string(destination, self.name);
        write_string(destination, self.value);

        Ok(())
    }
}

pub(crate) const READY_FOR_QUERY_IDENTIFIER: u8 = b'Z';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReadyForQuery {
    pub(crate) tx_status: u8, // 'I' (idle), 'T' (in transaction), 'E' (failed transaction)
}

impl Encode for ReadyForQuery {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(READY_FOR_QUERY_IDENTIFIER);

        destination.put_i32(5); // length (4-byte length + 1-byte payload)
        destination.put_u8(self.tx_status);
        Ok(())
    }
}

pub(crate) const BACKEND_KEY_DATA_IDENTIFIER: u8 = b'K';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BackendKeyData {
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
}

impl Encode for BackendKeyData {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.reserve(13);
        destination.put_u8(BACKEND_KEY_DATA_IDENTIFIER);
        destination.put_i32(12);
        destination.put_i32(self.process_id);
        destination.put_i32(self.secret_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_message_decode() {
        let mut data = BytesMut::new();
        data.put_u32(196608); // protocol version 3.0
        write_string(&mut data, "user");
        write_string(&mut data, "postgres");
        write_string(&mut data, "database");
        write_string(&mut data, "testdb");
        data.put_u8(0); // termination byte

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = StartupMessage::decode(&mut source).expect("failed to decode StartupMessage");

        assert_eq!(message.protocol_version, 196608);
        assert_eq!(
            message.parameters.get("user").expect("user param missing"),
            &"postgres"
        );
        assert_eq!(
            message
                .parameters
                .get("database")
                .expect("database param missing"),
            &"testdb"
        );
        assert!(source.is_empty());
    }

    #[test]
    fn test_startup_message_decode_unsupported_version() {
        let mut data = BytesMut::new();
        data.put_u32(262144); // protocol version 4.0
        data.put_u8(0); // termination byte

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = StartupMessage::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "unsupported protocol version: 4.0"
        );
    }

    #[test]
    fn test_startup_message_decode_insufficient_bytes() {
        let payload = b"\x00\x03\x00"; // 3 bytes
        let mut source = &payload[..];
        let result = StartupMessage::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "insufficient bytes for protocol version"
        );
    }

    #[test]
    fn test_startup_message_decode_not_terminated() {
        let mut data = BytesMut::new();
        data.put_u32(196608); // 3.0
        write_string(&mut data, "user");
        write_string(&mut data, "postgres");
        // missing trailing 0 terminator

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = StartupMessage::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "startup message parameters not terminated properly"
        );
    }

    #[test]
    fn test_authentication_ok_encode() {
        let mut buffer = BytesMut::new();
        AuthenticationOk
            .encode(&mut buffer)
            .expect("failed to encode AuthenticationOk");

        assert_eq!(buffer.to_vec(), vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    }

    #[test]
    fn test_parameter_status_encode() {
        let mut buffer = BytesMut::new();
        let parameter = ParameterStatus {
            name: "client_encoding",
            value: "UTF8",
        };
        parameter
            .encode(&mut buffer)
            .expect("failed to encode ParameterStatus");

        let expected_payload = b"client_encoding\0UTF8\0";
        let expected_len = (4 + expected_payload.len()) as i32;

        let mut expected = BytesMut::new();
        expected.put_u8(b'S');
        expected.put_i32(expected_len);
        expected.put_slice(expected_payload);

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_ready_for_query_encode() {
        let mut buffer = BytesMut::new();
        let ready = ReadyForQuery { tx_status: b'I' };
        ready
            .encode(&mut buffer)
            .expect("failed to encode ReadyForQuery");

        assert_eq!(buffer.to_vec(), vec![b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn test_backend_key_data_encode() {
        let mut buffer = BytesMut::new();
        let key_data = BackendKeyData {
            process_id: 1234,
            secret_key: 5678,
        };
        key_data
            .encode(&mut buffer)
            .expect("failed to encode BackendKeyData");

        let mut expected = vec![b'K', 0, 0, 0, 12];
        expected.extend_from_slice(&1234i32.to_be_bytes());
        expected.extend_from_slice(&5678i32.to_be_bytes());
        assert_eq!(buffer.to_vec(), expected);
    }
}
