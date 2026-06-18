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

use bytes::{BufMut, BytesMut};

use crate::pgwire::protocol::error::ErrorResponse;
use crate::pgwire::protocol::extended::{
    BIND_IDENTIFIER, Bind, BindComplete, CLOSE_IDENTIFIER, Close, CloseComplete,
    DESCRIBE_IDENTIFIER, DataRow, Describe, EXECUTE_IDENTIFIER, Execute, NoData, PARSE_IDENTIFIER,
    ParameterDescription, Parse, ParseComplete, PortalSuspended, RowDescription,
};
use crate::pgwire::protocol::handshake::{
    AuthenticationOk, BackendKeyData, ParameterStatus, ReadyForQuery, StartupMessage,
};
use crate::pgwire::protocol::query::{
    CommandComplete, EmptyQueryResponse, QUERY_IDENTIFIER, Query,
};
use crate::pgwire::protocol::terminate::{TERMINATE_IDENTIFIER, Terminate};

pub(crate) trait Decode<'a>: Sized {
    /// Decodes a message from the payload bytes slice.
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error>;
}

pub(crate) trait Encode {
    /// Encodes a message into the destination byte buffer.
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error>;
}

/// Helper function to parse a null-terminated string from a byte buffer slice.
pub(crate) fn read_string<'a>(source: &mut &'a [u8]) -> Result<&'a str, anyhow::Error> {
    let null_index = source
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| anyhow::anyhow!("string is not null-terminated"))?;
    let bytes = &source[..null_index];
    *source = &source[null_index + 1..]; // advance past the null byte
    let string = std::str::from_utf8(bytes)?;
    Ok(string)
}

pub(crate) fn read_i16(source: &mut &[u8]) -> Result<i16, anyhow::Error> {
    if source.len() < 2 {
        return Err(anyhow::anyhow!("insufficient bytes for i16"));
    }
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&source[..2]);
    *source = &source[2..];
    Ok(i16::from_be_bytes(bytes))
}

pub(crate) fn read_i32(source: &mut &[u8]) -> Result<i32, anyhow::Error> {
    if source.len() < 4 {
        return Err(anyhow::anyhow!("insufficient bytes for i32"));
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&source[..4]);
    *source = &source[4..];
    Ok(i32::from_be_bytes(bytes))
}

pub(crate) fn read_u32(source: &mut &[u8]) -> Result<u32, anyhow::Error> {
    if source.len() < 4 {
        return Err(anyhow::anyhow!("insufficient bytes for u32"));
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&source[..4]);
    *source = &source[4..];
    Ok(u32::from_be_bytes(bytes))
}

pub(crate) fn read_u8(source: &mut &[u8]) -> Result<u8, anyhow::Error> {
    if source.is_empty() {
        return Err(anyhow::anyhow!("insufficient bytes for u8"));
    }
    let byte = source[0];
    *source = &source[1..];
    Ok(byte)
}

/// Helper function to write a null-terminated string to a byte buffer.
pub(crate) fn write_string(destination: &mut BytesMut, string: &str) {
    destination.reserve(string.len() + 1);
    destination.put_slice(string.as_bytes());
    destination.put_u8(0);
}

pub(crate) const SYNC_IDENTIFIER: u8 = b'S';
pub(crate) const FLUSH_IDENTIFIER: u8 = b'H';

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SyncMessage;

impl<'a> Decode<'a> for SyncMessage {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        if !source.is_empty() {
            return Err(anyhow::anyhow!("Sync message should not contain payload"));
        }
        Ok(SyncMessage)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FlushMessage;

impl<'a> Decode<'a> for FlushMessage {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        if !source.is_empty() {
            return Err(anyhow::anyhow!("Flush message should not contain payload"));
        }
        Ok(FlushMessage)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrontendMessage {
    Startup(StartupMessage),
    SSLRequest,
    GSSENCRequest,
    CancelRequest {
        process_id: u32,
        secret_key: Vec<u8>,
    },
    Query(Query),
    Parse(Parse),
    Bind(Bind),
    Execute(Execute),
    Describe(Describe),
    Sync(SyncMessage),
    Flush(FlushMessage),
    Close(Close),
    Terminate(Terminate),
}

impl FrontendMessage {
    /// Decodes a frontend message from a regular packet (type byte + payload slice).
    pub(crate) fn decode(type_byte: u8, mut payload: &[u8]) -> Result<Self, anyhow::Error> {
        match type_byte {
            QUERY_IDENTIFIER => Ok(FrontendMessage::Query(Query::decode(&mut payload)?)),
            PARSE_IDENTIFIER => Ok(FrontendMessage::Parse(Parse::decode(&mut payload)?)),
            BIND_IDENTIFIER => Ok(FrontendMessage::Bind(Bind::decode(&mut payload)?)),
            EXECUTE_IDENTIFIER => Ok(FrontendMessage::Execute(Execute::decode(&mut payload)?)),
            DESCRIBE_IDENTIFIER => Ok(FrontendMessage::Describe(Describe::decode(&mut payload)?)),
            CLOSE_IDENTIFIER => Ok(FrontendMessage::Close(Close::decode(&mut payload)?)),
            SYNC_IDENTIFIER => Ok(FrontendMessage::Sync(SyncMessage::decode(&mut payload)?)),
            FLUSH_IDENTIFIER => Ok(FrontendMessage::Flush(FlushMessage::decode(&mut payload)?)),
            TERMINATE_IDENTIFIER => {
                Ok(FrontendMessage::Terminate(Terminate::decode(&mut payload)?))
            }
            other => Err(anyhow::anyhow!("unknown type byte: {}", other as char)),
        }
    }

    /// Decodes a startup-phase packet payload slice.
    pub(crate) fn decode_startup(mut payload: &[u8]) -> Result<Self, anyhow::Error> {
        let mut code_payload = payload;
        let code = read_u32(&mut code_payload)
            .map_err(|_| anyhow::anyhow!("insufficient bytes for startup packet"))?;

        if code == 80877103 {
            if !code_payload.is_empty() {
                return Err(anyhow::anyhow!("invalid length for SSLRequest"));
            }
            return Ok(FrontendMessage::SSLRequest);
        }

        if code == 80877104 {
            if !code_payload.is_empty() {
                return Err(anyhow::anyhow!("invalid length for GSSENCRequest"));
            }
            return Ok(FrontendMessage::GSSENCRequest);
        }

        if code == 80877102 {
            if code_payload.len() < 8 {
                return Err(anyhow::anyhow!("insufficient bytes for CancelRequest"));
            }
            let process_id = read_u32(&mut code_payload)?;
            let secret_key = code_payload.to_vec();
            return Ok(FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            });
        }

        // Otherwise, decode StartupMessage.
        let startup = StartupMessage::decode(&mut payload)?;
        Ok(FrontendMessage::Startup(startup))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BackendMessage<'a> {
    AuthenticationOk,
    ReadyForQuery(ReadyForQuery),
    ParameterStatus(ParameterStatus<'a>),
    RowDescription(RowDescription<'a>),
    DataRow(DataRow<'a>),
    CommandComplete(CommandComplete<'a>),
    ParseComplete,
    BindComplete,
    NoData,
    PortalSuspended,
    EmptyQueryResponse,
    ErrorResponse(ErrorResponse<'a>),
    BackendKeyData(BackendKeyData),
    ParameterDescription(ParameterDescription),
    CloseComplete,
}

impl<'a> Encode for BackendMessage<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        match self {
            BackendMessage::AuthenticationOk => AuthenticationOk.encode(destination),
            BackendMessage::ReadyForQuery(message) => message.encode(destination),
            BackendMessage::ParameterStatus(message) => message.encode(destination),
            BackendMessage::RowDescription(message) => message.encode(destination),
            BackendMessage::DataRow(message) => message.encode(destination),
            BackendMessage::CommandComplete(message) => message.encode(destination),
            BackendMessage::ParseComplete => ParseComplete.encode(destination),
            BackendMessage::BindComplete => BindComplete.encode(destination),
            BackendMessage::NoData => NoData.encode(destination),
            BackendMessage::PortalSuspended => PortalSuspended.encode(destination),
            BackendMessage::EmptyQueryResponse => EmptyQueryResponse.encode(destination),
            BackendMessage::ErrorResponse(message) => message.encode(destination),
            BackendMessage::BackendKeyData(message) => message.encode(destination),
            BackendMessage::ParameterDescription(message) => message.encode(destination),
            BackendMessage::CloseComplete => CloseComplete.encode(destination),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frontend_decode_startup_ssl() {
        let payload = vec![0x04, 0xd2, 0x16, 0x2f]; // 80877103 in hex
        let message = FrontendMessage::decode_startup(&payload)
            .expect("failed to decode startup SSL request");
        assert_eq!(message, FrontendMessage::SSLRequest);
    }

    #[test]
    fn test_frontend_decode_startup_gss() {
        let payload = vec![0x04, 0xd2, 0x16, 0x30]; // 80877104 in hex
        let message = FrontendMessage::decode_startup(&payload)
            .expect("failed to decode startup GSS request");
        assert_eq!(message, FrontendMessage::GSSENCRequest);
    }

    #[test]
    fn test_frontend_decode_startup_cancel() {
        let mut payload = vec![0x04, 0xd2, 0x16, 0x2e]; // CancelRequest code
        payload.extend_from_slice(&1234u32.to_be_bytes()); // PID
        payload.extend_from_slice(b"secretkey"); // Secret Key
        let message =
            FrontendMessage::decode_startup(&payload).expect("failed to decode CancelRequest");
        assert_eq!(
            message,
            FrontendMessage::CancelRequest {
                process_id: 1234,
                secret_key: b"secretkey".to_vec()
            }
        );
    }

    #[test]
    fn test_frontend_decode_startup_malformed() {
        let payload = vec![0, 0, 0]; // 3 bytes
        let result = FrontendMessage::decode_startup(&payload);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "insufficient bytes for startup packet"
        );
    }

    #[test]
    fn test_frontend_decode_sync() {
        let message = FrontendMessage::decode(b'S', &[]).expect("failed to decode sync");
        assert_eq!(message, FrontendMessage::Sync(SyncMessage));
    }

    #[test]
    fn test_backend_encode_parse_complete() {
        let mut buffer = BytesMut::new();
        BackendMessage::ParseComplete
            .encode(&mut buffer)
            .expect("failed to encode ParseComplete");
        assert_eq!(buffer.to_vec(), vec![b'1', 0, 0, 0, 4]);
    }

    #[test]
    fn test_write_string_multibyte() {
        let mut buffer = BytesMut::new();
        let test_str = "Hello, 世界! 🌍"; // "世界" has 2 x 3 bytes, "🌍" has 4 bytes

        // Expected byte length of the string in UTF-8 is 19 bytes.
        // With the null terminator, it should occupy exactly 20 bytes.
        assert_eq!(test_str.len(), 19);

        write_string(&mut buffer, test_str);

        let mut expected = test_str.as_bytes().to_vec();
        expected.push(0); // null terminator

        assert_eq!(buffer.to_vec(), expected);
        assert_eq!(buffer.len(), 20);
    }

    #[test]
    fn test_backend_encode_parameter_description() {
        let mut buffer = BytesMut::new();
        BackendMessage::ParameterDescription(ParameterDescription {
            param_types: vec![23],
        })
        .encode(&mut buffer)
        .expect("failed to encode ParameterDescription");
        assert_eq!(buffer.to_vec(), vec![b't', 0, 0, 0, 10, 0, 1, 0, 0, 0, 23]);
    }

    #[test]
    fn test_backend_encode_close_complete() {
        let mut buffer = BytesMut::new();
        BackendMessage::CloseComplete
            .encode(&mut buffer)
            .expect("failed to encode CloseComplete");
        assert_eq!(buffer.to_vec(), vec![b'3', 0, 0, 0, 4]);
    }
}
