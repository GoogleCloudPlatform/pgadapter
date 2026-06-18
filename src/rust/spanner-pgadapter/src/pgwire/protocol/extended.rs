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

use crate::pgwire::protocol::{
    Decode, Encode, read_i16, read_i32, read_string, read_u8, read_u32, write_string,
};
use bytes::{BufMut, BytesMut};

// --- Frontend Messages ---

pub(crate) const PARSE_IDENTIFIER: u8 = b'P';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Parse {
    pub(crate) name: String,
    pub(crate) query: String,
    pub(crate) param_types: Vec<u32>,
}

impl<'a> Decode<'a> for Parse {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let name = read_string(source)?.to_string();
        let query = read_string(source)?.to_string();
        let num_parameters = read_i16(source)? as usize;
        if source.len() < num_parameters * 4 {
            return Err(anyhow::anyhow!("insufficient bytes for parameter OIDs"));
        }
        let mut param_types = Vec::with_capacity(num_parameters);
        for _ in 0..num_parameters {
            param_types.push(read_u32(source)?);
        }
        Ok(Parse {
            name,
            query,
            param_types,
        })
    }
}

pub(crate) const BIND_IDENTIFIER: u8 = b'B';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Bind {
    pub(crate) portal: String,
    pub(crate) statement: String,
    pub(crate) param_formats: Vec<i16>,
    pub(crate) params: Vec<Option<bytes::Bytes>>,
    pub(crate) result_formats: Vec<i16>,
}

impl<'a> Decode<'a> for Bind {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let portal = read_string(source)?.to_string();
        let statement = read_string(source)?.to_string();

        let num_formats = read_i16(source)? as usize;
        if source.len() < num_formats * 2 {
            return Err(anyhow::anyhow!("insufficient bytes for param format codes"));
        }
        let mut param_formats = Vec::with_capacity(num_formats);
        for _ in 0..num_formats {
            param_formats.push(read_i16(source)?);
        }

        let num_parameters = read_i16(source)? as usize;
        let mut params = Vec::with_capacity(num_parameters);
        for _ in 0..num_parameters {
            let len = read_i32(source)?;
            if len == -1 {
                params.push(None);
            } else {
                let len = len as usize;
                if source.len() < len {
                    return Err(anyhow::anyhow!("insufficient bytes for parameter value"));
                }
                let value = &source[..len];
                *source = &source[len..];
                params.push(Some(bytes::Bytes::copy_from_slice(value)));
            }
        }

        let num_result_formats = read_i16(source)? as usize;
        if source.len() < num_result_formats * 2 {
            return Err(anyhow::anyhow!(
                "insufficient bytes for result format codes"
            ));
        }
        let mut result_formats = Vec::with_capacity(num_result_formats);
        for _ in 0..num_result_formats {
            result_formats.push(read_i16(source)?);
        }

        Ok(Bind {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        })
    }
}

pub(crate) const EXECUTE_IDENTIFIER: u8 = b'E';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Execute {
    pub(crate) portal: String,
    pub(crate) max_rows: i32,
}

impl<'a> Decode<'a> for Execute {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let portal = read_string(source)?.to_string();
        let max_rows = read_i32(source)?;
        Ok(Execute { portal, max_rows })
    }
}

pub(crate) const DESCRIBE_IDENTIFIER: u8 = b'D';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Describe {
    pub(crate) desc_type: u8, // 'S' for prepared statement, 'P' for portal
    pub(crate) name: String,
}

impl<'a> Decode<'a> for Describe {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let desc_type = read_u8(source)?;
        if desc_type != b'S' && desc_type != b'P' {
            return Err(anyhow::anyhow!("invalid describe type: {}", desc_type));
        }
        let name = read_string(source)?.to_string();
        Ok(Describe { desc_type, name })
    }
}

pub(crate) const CLOSE_IDENTIFIER: u8 = b'C';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct Close {
    pub(crate) close_type: u8, // 'S' for prepared statement, 'P' for portal
    pub(crate) name: String,
}

impl<'a> Decode<'a> for Close {
    fn decode(source: &mut &'a [u8]) -> Result<Self, anyhow::Error> {
        let close_type = read_u8(source)?;
        if close_type != b'S' && close_type != b'P' {
            return Err(anyhow::anyhow!("invalid close type: {}", close_type));
        }
        let name = read_string(source)?.to_string();
        Ok(Close { close_type, name })
    }
}

// --- Backend Messages ---

pub(crate) const PARSE_COMPLETE_IDENTIFIER: u8 = b'1';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct ParseComplete;

impl Encode for ParseComplete {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(PARSE_COMPLETE_IDENTIFIER);

        destination.put_i32(4); // length is always 4
        Ok(())
    }
}

pub(crate) const BIND_COMPLETE_IDENTIFIER: u8 = b'2';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BindComplete;

impl Encode for BindComplete {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(BIND_COMPLETE_IDENTIFIER);

        destination.put_i32(4); // length is always 4
        Ok(())
    }
}

pub(crate) const CLOSE_COMPLETE_IDENTIFIER: u8 = b'3';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct CloseComplete;

impl Encode for CloseComplete {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(CLOSE_COMPLETE_IDENTIFIER);

        destination.put_i32(4); // length is always 4
        Ok(())
    }
}

pub(crate) const NO_DATA_IDENTIFIER: u8 = b'n';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct NoData;

impl Encode for NoData {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(NO_DATA_IDENTIFIER);

        destination.put_i32(4); // length is always 4
        Ok(())
    }
}

pub(crate) const PORTAL_SUSPENDED_IDENTIFIER: u8 = b's';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct PortalSuspended;

impl Encode for PortalSuspended {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        destination.put_u8(PORTAL_SUSPENDED_IDENTIFIER);

        destination.put_i32(4); // length is always 4
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct FieldDescription<'a> {
    pub(crate) name: &'a str,
    pub(crate) table_oid: u32,
    pub(crate) column_index: i16,
    pub(crate) type_oid: u32,
    pub(crate) type_size: i16,
    pub(crate) type_modifier: i32,
    pub(crate) format_code: i16,
}

pub(crate) const ROW_DESCRIPTION_IDENTIFIER: u8 = b'T';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct RowDescription<'a> {
    pub(crate) fields: Vec<FieldDescription<'a>>,
}

impl<'a> Encode for RowDescription<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let fields_len: usize = self
            .fields
            .iter()
            .map(|field| field.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2)
            .sum();
        let payload_len = 4 + 2 + fields_len;

        destination.reserve(1 + payload_len);
        destination.put_u8(ROW_DESCRIPTION_IDENTIFIER);
        destination.put_i32(payload_len as i32);
        destination.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            write_string(destination, field.name);
            destination.put_u32(field.table_oid);
            destination.put_i16(field.column_index);
            destination.put_u32(field.type_oid);
            destination.put_i16(field.type_size);
            destination.put_i32(field.type_modifier);
            destination.put_i16(field.format_code);
        }

        Ok(())
    }
}

pub(crate) const DATA_ROW_IDENTIFIER: u8 = b'D';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct DataRow<'a> {
    pub(crate) values: Vec<Option<&'a [u8]>>,
}

impl<'a> Encode for DataRow<'a> {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let values_len: usize = self
            .values
            .iter()
            .map(|value| match value {
                None => 4,
                Some(bytes) => 4 + bytes.len(),
            })
            .sum();
        let payload_len = 4 + 2 + values_len;

        destination.reserve(1 + payload_len);
        destination.put_u8(DATA_ROW_IDENTIFIER);
        destination.put_i32(payload_len as i32);
        destination.put_i16(self.values.len() as i16);

        for value in &self.values {
            match value {
                None => {
                    destination.put_i32(-1);
                }
                Some(bytes) => {
                    destination.put_i32(bytes.len() as i32);
                    destination.put_slice(bytes);
                }
            }
        }

        Ok(())
    }
}

pub(crate) const PARAMETER_DESCRIPTION_IDENTIFIER: u8 = b't';

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct ParameterDescription {
    pub(crate) param_types: Vec<u32>,
}

impl Encode for ParameterDescription {
    fn encode(&self, destination: &mut BytesMut) -> Result<(), anyhow::Error> {
        let payload_len = 2 + self.param_types.len() * 4;
        destination.reserve(1 + 4 + payload_len);
        destination.put_u8(PARAMETER_DESCRIPTION_IDENTIFIER);
        destination.put_i32((4 + payload_len) as i32);
        destination.put_i16(self.param_types.len() as i16);
        for &param_type in &self.param_types {
            destination.put_u32(param_type);
        }
        Ok(())
    }
}

// --- Unit Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decode() {
        let mut data = BytesMut::new();
        write_string(&mut data, "stmt1");
        write_string(&mut data, "SELECT * FROM users WHERE id = $1");
        data.put_i16(1);
        data.put_u32(23); // OID for int4

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = Parse::decode(&mut source).expect("failed to decode Parse");

        assert_eq!(message.name, "stmt1");
        assert_eq!(message.query, "SELECT * FROM users WHERE id = $1");
        assert_eq!(message.param_types, vec![23]);
        assert!(source.is_empty());
    }

    #[test]
    fn test_bind_decode() {
        let mut data = BytesMut::new();
        write_string(&mut data, "portal1");
        write_string(&mut data, "stmt1");
        data.put_i16(1); // 1 format code
        data.put_i16(1); // binary format
        data.put_i16(2); // 2 parameters

        // Param 1: non-null value "hello" (binary length 5)
        data.put_i32(5);
        data.put_slice(b"hello");

        // Param 2: null value
        data.put_i32(-1);

        data.put_i16(0); // 0 result formats

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = Bind::decode(&mut source).expect("failed to decode Bind");

        assert_eq!(message.portal, "portal1");
        assert_eq!(message.statement, "stmt1");
        assert_eq!(message.param_formats, vec![1]);
        assert_eq!(
            message.params,
            vec![Some(bytes::Bytes::from_static(b"hello")), None]
        );
        let expected_result_formats: Vec<i16> = vec![];
        assert_eq!(message.result_formats, expected_result_formats);
        assert!(source.is_empty());
    }

    #[test]
    fn test_execute_decode() {
        let mut data = BytesMut::new();
        write_string(&mut data, "portal1");
        data.put_i32(100);

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = Execute::decode(&mut source).expect("failed to decode Execute");

        assert_eq!(message.portal, "portal1");
        assert_eq!(message.max_rows, 100);
        assert!(source.is_empty());
    }

    #[test]
    fn test_describe_decode() {
        let mut data = BytesMut::new();
        data.put_u8(b'S');
        write_string(&mut data, "stmt1");

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = Describe::decode(&mut source).expect("failed to decode Describe");

        assert_eq!(message.desc_type, b'S');
        assert_eq!(message.name, "stmt1");
        assert!(source.is_empty());
    }

    #[test]
    fn test_close_decode() {
        let mut data = BytesMut::new();
        data.put_u8(b'P');
        write_string(&mut data, "portal1");

        let payload = data.freeze();
        let mut source = &payload[..];
        let message = Close::decode(&mut source).expect("failed to decode Close");

        assert_eq!(message.close_type, b'P');
        assert_eq!(message.name, "portal1");
        assert!(source.is_empty());
    }

    #[test]
    fn test_parse_complete_encode() {
        let mut buffer = BytesMut::new();
        ParseComplete
            .encode(&mut buffer)
            .expect("failed to encode ParseComplete");
        assert_eq!(buffer.to_vec(), vec![b'1', 0, 0, 0, 4]);
    }

    #[test]
    fn test_bind_complete_encode() {
        let mut buffer = BytesMut::new();
        BindComplete
            .encode(&mut buffer)
            .expect("failed to encode BindComplete");
        assert_eq!(buffer.to_vec(), vec![b'2', 0, 0, 0, 4]);
    }

    #[test]
    fn test_row_description_encode() {
        let mut buffer = BytesMut::new();
        let fields = vec![FieldDescription {
            name: "id",
            table_oid: 1234,
            column_index: 1,
            type_oid: 23, // int4
            type_size: 4,
            type_modifier: -1,
            format_code: 1, // binary
        }];
        let row_description = RowDescription { fields };
        row_description
            .encode(&mut buffer)
            .expect("failed to encode RowDescription");

        let mut expected = BytesMut::new();
        expected.put_u8(b'T');
        expected.put_i32(4 + 2 + ("id".len() + 1 + 4 + 2 + 4 + 2 + 4 + 2) as i32);
        expected.put_i16(1);
        write_string(&mut expected, "id");
        expected.put_u32(1234);
        expected.put_i16(1);
        expected.put_u32(23);
        expected.put_i16(4);
        expected.put_i32(-1);
        expected.put_i16(1);

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_data_row_encode() {
        let mut buffer = BytesMut::new();
        let data_row = DataRow {
            values: vec![Some(b"hello" as &[u8]), None],
        };
        data_row
            .encode(&mut buffer)
            .expect("failed to encode DataRow");

        let mut expected = BytesMut::new();
        expected.put_u8(b'D');
        expected.put_i32(4 + 2 + 4 + 5 + 4);
        expected.put_i16(2);
        expected.put_i32(5);
        expected.put_slice(b"hello");
        expected.put_i32(-1);

        assert_eq!(buffer.to_vec(), expected.to_vec());
    }

    #[test]
    fn test_parse_decode_malformed() {
        let mut data = BytesMut::new();
        write_string(&mut data, "stmt1");
        write_string(&mut data, "SELECT * FROM users WHERE id = $1");
        data.put_i16(2); // Claims 2 params
        data.put_u32(23); // Only supplies 1 param (4 bytes instead of 8)

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = Parse::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "insufficient bytes for parameter OIDs"
        );
    }

    #[test]
    fn test_bind_decode_malformed_value() {
        let mut data = BytesMut::new();
        write_string(&mut data, "portal1");
        write_string(&mut data, "stmt1");
        data.put_i16(0); // 0 formats
        data.put_i16(1); // 1 param
        data.put_i32(10); // Claims length is 10
        data.put_slice(b"short"); // Only provides 5 bytes

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = Bind::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "insufficient bytes for parameter value"
        );
    }

    #[test]
    fn test_describe_decode_invalid_type() {
        let mut data = BytesMut::new();
        data.put_u8(b'X'); // Invalid describe type ('X' instead of 'S' or 'P')
        write_string(&mut data, "stmt1");

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = Describe::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "invalid describe type: 88"
        );
    }

    #[test]
    fn test_close_decode_invalid_type() {
        let mut data = BytesMut::new();
        data.put_u8(b'X'); // Invalid close type
        write_string(&mut data, "portal1");

        let payload = data.freeze();
        let mut source = &payload[..];
        let result = Close::decode(&mut source);
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("expected error").to_string(),
            "invalid close type: 88"
        );
    }

    #[test]
    fn test_parameter_description_encode() {
        let msg = ParameterDescription {
            param_types: vec![23, 1043],
        };
        let mut dest = BytesMut::new();
        msg.encode(&mut dest).unwrap();

        assert_eq!(dest[0], b't');
        let len = i32::from_be_bytes([dest[1], dest[2], dest[3], dest[4]]);
        assert_eq!(len, 14); // 4 + 2 + 8
        let count = i16::from_be_bytes([dest[5], dest[6]]);
        assert_eq!(count, 2);
        let oid1 = u32::from_be_bytes([dest[7], dest[8], dest[9], dest[10]]);
        assert_eq!(oid1, 23);
        let oid2 = u32::from_be_bytes([dest[11], dest[12], dest[13], dest[14]]);
        assert_eq!(oid2, 1043);
    }

    #[test]
    fn test_close_complete_encode() {
        let msg = CloseComplete;
        let mut dest = BytesMut::new();
        msg.encode(&mut dest).unwrap();
        assert_eq!(dest.to_vec(), vec![b'3', 0, 0, 0, 4]);
    }
}
