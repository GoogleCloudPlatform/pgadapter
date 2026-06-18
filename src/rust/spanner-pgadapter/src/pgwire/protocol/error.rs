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

use crate::pgwire::protocol::{Encode, write_string};
use bytes::{BufMut, BytesMut};

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct ErrorResponse<'a> {
    pub(crate) fields: Vec<(u8, &'a str)>,
}

impl<'a> ErrorResponse<'a> {
    /// Creates a new `ErrorResponse` with standard required fields: Severity, SQLSTATE code, and Message.
    pub(crate) fn new(severity: &'a str, code: &'a str, message: &'a str) -> Self {
        Self {
            fields: vec![(b'S', severity), (b'C', code), (b'M', message)],
        }
    }

    /// Adds a detail field to the `ErrorResponse`.
    pub(crate) fn with_detail(mut self, detail: &'a str) -> Self {
        self.fields.push((b'D', detail));
        self
    }

    /// Adds a hint field to the `ErrorResponse`.
    pub(crate) fn with_hint(mut self, hint: &'a str) -> Self {
        self.fields.push((b'H', hint));
        self
    }

    /// Adds any arbitrary key-value field to the `ErrorResponse`.
    pub(crate) fn with_field(mut self, field_type: u8, value: &'a str) -> Self {
        self.fields.push((field_type, value));
        self
    }
}

pub(crate) const ERROR_IDENTIFIER: u8 = b'E';

impl<'a> Encode for ErrorResponse<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let fields_len: usize = self
            .fields
            .iter()
            .map(|(_, value)| 1 + value.len() + 1)
            .sum();
        let payload_len = 4 + fields_len + 1; // 4-byte length + fields + 1-byte terminator (0)

        destination.reserve(1 + payload_len);
        destination.put_u8(ERROR_IDENTIFIER);
        destination.put_i32(payload_len as i32);

        for &(field_type, value) in &self.fields {
            destination.put_u8(field_type);
            write_string(destination, value);
        }

        // Terminating zero byte for the fields list
        destination.put_u8(0);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_encode_standard() {
        let mut buffer = BytesMut::new();
        let error = ErrorResponse::new("FATAL", "57P01", "admin shutdown");
        error
            .encode(&mut buffer)
            .expect("failed to encode ErrorResponse");

        let mut expected = BytesMut::new();
        expected.put_u8(b'E');

        let start_pos = expected.len();
        expected.put_i32(0); // length placeholder

        expected.put_u8(b'S');
        write_string(&mut expected, "FATAL");

        expected.put_u8(b'C');
        write_string(&mut expected, "57P01");

        expected.put_u8(b'M');
        write_string(&mut expected, "admin shutdown");

        expected.put_u8(0);

        let end_pos = expected.len();
        let len = (end_pos - start_pos) as i32;
        expected[start_pos..start_pos + 4].copy_from_slice(&len.to_be_bytes());

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_error_response_encode_with_optional_fields() {
        let mut buffer = BytesMut::new();
        let error = ErrorResponse::new("ERROR", "28000", "invalid password")
            .with_detail("check configuration")
            .with_hint("try resetting the secret key");
        error
            .encode(&mut buffer)
            .expect("failed to encode ErrorResponse");

        let mut expected = BytesMut::new();
        expected.put_u8(b'E');

        let start_pos = expected.len();
        expected.put_i32(0); // length placeholder

        expected.put_u8(b'S');
        write_string(&mut expected, "ERROR");

        expected.put_u8(b'C');
        write_string(&mut expected, "28000");

        expected.put_u8(b'M');
        write_string(&mut expected, "invalid password");

        expected.put_u8(b'D');
        write_string(&mut expected, "check configuration");

        expected.put_u8(b'H');
        write_string(&mut expected, "try resetting the secret key");

        expected.put_u8(0);

        let end_pos = expected.len();
        let len = (end_pos - start_pos) as i32;
        expected[start_pos..start_pos + 4].copy_from_slice(&len.to_be_bytes());

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_error_response_encode_with_custom_field() {
        let mut buffer = BytesMut::new();
        let error = ErrorResponse::new("ERROR", "42601", "syntax error")
            .with_field(b'F', "parser.c")
            .with_field(b'L', "42");
        error
            .encode(&mut buffer)
            .expect("failed to encode ErrorResponse");

        let mut expected = BytesMut::new();
        expected.put_u8(b'E');

        let start_pos = expected.len();
        expected.put_i32(0); // length placeholder

        expected.put_u8(b'S');
        write_string(&mut expected, "ERROR");

        expected.put_u8(b'C');
        write_string(&mut expected, "42601");

        expected.put_u8(b'M');
        write_string(&mut expected, "syntax error");

        expected.put_u8(b'F');
        write_string(&mut expected, "parser.c");

        expected.put_u8(b'L');
        write_string(&mut expected, "42");

        expected.put_u8(0);

        let end_pos = expected.len();
        let len = (end_pos - start_pos) as i32;
        expected[start_pos..start_pos + 4].copy_from_slice(&len.to_be_bytes());

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }
}
