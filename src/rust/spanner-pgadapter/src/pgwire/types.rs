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

#![allow(dead_code)]

use bytes::{BufMut, Bytes, BytesMut};
use google_cloud_spanner::Decimal;
use google_cloud_spanner::value::{
    FromValue, Kind as SpannerValueKind, ToValue, Type as SpannerType, TypeCode as SpannerTypeCode,
    Value as SpannerValue,
};
use serde_json;

// PostgreSQL Type OIDs
pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_INT8: u32 = 20;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_DATE: u32 = 1082;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;
pub const OID_NUMERIC: u32 = 1700;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_JSONB: u32 = 3802;

// PostgreSQL Array Type OIDs
pub const OID_ARRAY_BOOL: u32 = 1000;
pub const OID_ARRAY_INT4: u32 = 1007;
pub const OID_ARRAY_TEXT: u32 = 1009;
pub const OID_ARRAY_INT8: u32 = 1016;
pub const OID_ARRAY_TIMESTAMP: u32 = 1115;
pub const OID_ARRAY_FLOAT4: u32 = 1021;
pub const OID_ARRAY_FLOAT8: u32 = 1022;
pub const OID_ARRAY_NUMERIC: u32 = 1231;
pub const OID_ARRAY_DATE: u32 = 1182;
pub const OID_ARRAY_TIMESTAMPTZ: u32 = 1185;
pub const OID_ARRAY_JSONB: u32 = 3807;

/// Converts a Spanner raw Value and Type into a pg-wire compatible byte representation.
pub fn spanner_value_to_pgwire_bytes(
    value: &SpannerValue,
    spanner_type: &SpannerType,
    oid: u32,
    format_code: i16,
) -> Result<Option<Bytes>, anyhow::Error> {
    if value.kind() == SpannerValueKind::Null {
        return Ok(None);
    }

    if format_code == 1 {
        spanner_value_to_pgwire_binary(value, spanner_type, oid)
    } else {
        spanner_value_to_pgwire_text(value, spanner_type, oid)
    }
}

fn spanner_value_to_pgwire_binary(
    value: &SpannerValue,
    spanner_type: &SpannerType,
    oid: u32,
) -> Result<Option<Bytes>, anyhow::Error> {
    match oid {
        OID_BOOL => {
            let decoded_value = bool::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to bool: {}", e))?;
            Ok(Some(Bytes::from(vec![if decoded_value { 1 } else { 0 }])))
        }
        OID_INT4 => {
            let decoded_value = i64::from_value(value, spanner_type).map_err(|e| {
                anyhow::anyhow!("Failed to convert Spanner value to i64 for INT4: {}", e)
            })?;
            let val_i32 = i32::try_from(decoded_value).map_err(|e| {
                anyhow::anyhow!("Value {} overflows INT4 OID: {}", decoded_value, e)
            })?;
            Ok(Some(Bytes::from(val_i32.to_be_bytes().to_vec())))
        }
        OID_INT8 => {
            let decoded_value = i64::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to i64: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_be_bytes().to_vec())))
        }
        OID_FLOAT4 => {
            let decoded_value = f32::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to f32: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_be_bytes().to_vec())))
        }
        OID_FLOAT8 => {
            let decoded_value = f64::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to f64: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_be_bytes().to_vec())))
        }
        OID_BYTEA => {
            let decoded_value = Vec::<u8>::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to Bytes: {}", e))?;
            Ok(Some(Bytes::from(decoded_value)))
        }
        OID_TEXT | OID_VARCHAR => {
            let decoded_value = String::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to String: {}", e))?;
            Ok(Some(Bytes::from(decoded_value)))
        }
        OID_JSONB => {
            let decoded_value = String::from_value(value, spanner_type).map_err(|e| {
                anyhow::anyhow!("Failed to convert Spanner value to String for JSON: {}", e)
            })?;
            let mut payload = Vec::with_capacity(1 + decoded_value.len());
            payload.push(1); // JSONB version prefix
            payload.extend_from_slice(decoded_value.as_bytes());
            Ok(Some(Bytes::from(payload)))
        }
        OID_ARRAY_BOOL
        | OID_ARRAY_INT8
        | OID_ARRAY_INT4
        | OID_ARRAY_TEXT
        | OID_ARRAY_FLOAT8
        | OID_ARRAY_FLOAT4
        | OID_ARRAY_NUMERIC
        | OID_ARRAY_DATE
        | OID_ARRAY_TIMESTAMPTZ
        | OID_ARRAY_JSONB => {
            let elem_oid = match oid {
                OID_ARRAY_BOOL => OID_BOOL,
                OID_ARRAY_INT8 => OID_INT8,
                OID_ARRAY_INT4 => OID_INT4,
                OID_ARRAY_TEXT => OID_TEXT,
                OID_ARRAY_FLOAT8 => OID_FLOAT8,
                OID_ARRAY_FLOAT4 => OID_FLOAT4,
                OID_ARRAY_NUMERIC => OID_NUMERIC,
                OID_ARRAY_DATE => OID_DATE,
                OID_ARRAY_TIMESTAMPTZ => OID_TIMESTAMPTZ,
                OID_ARRAY_JSONB => OID_JSONB,
                _ => OID_TEXT,
            };
            spanner_array_to_pgwire_binary(value, spanner_type, elem_oid)
        }
        _ => {
            // Fallback to text representation
            spanner_value_to_pgwire_text(value, spanner_type, oid)
        }
    }
}

fn spanner_value_to_pgwire_text(
    value: &SpannerValue,
    spanner_type: &SpannerType,
    oid: u32,
) -> Result<Option<Bytes>, anyhow::Error> {
    match oid {
        OID_BOOL => {
            let decoded_value = bool::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to bool: {}", e))?;
            Ok(Some(Bytes::from(if decoded_value { "t" } else { "f" })))
        }
        OID_INT4 | OID_INT8 => {
            let decoded_value = i64::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to i64: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_string())))
        }
        OID_FLOAT4 => {
            let decoded_value = f32::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to f32: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_string())))
        }
        OID_FLOAT8 => {
            let decoded_value = f64::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to f64: {}", e))?;
            Ok(Some(Bytes::from(decoded_value.to_string())))
        }
        OID_TEXT | OID_VARCHAR => {
            let decoded_value = String::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to String: {}", e))?;
            Ok(Some(Bytes::from(decoded_value)))
        }
        OID_BYTEA => {
            let decoded_value = Vec::<u8>::from_value(value, spanner_type)
                .map_err(|e| anyhow::anyhow!("Failed to convert Spanner value to Bytes: {}", e))?;
            let hex_str = decoded_value
                .iter()
                .map(|x| format!("{:02x}", x))
                .collect::<String>();
            Ok(Some(Bytes::from(format!("\\x{}", hex_str))))
        }
        OID_NUMERIC => {
            let decoded_value = Decimal::from_value(value, spanner_type).map_err(|e| {
                anyhow::anyhow!("Failed to convert Spanner value to Decimal: {}", e)
            })?;
            Ok(Some(Bytes::from(decoded_value.to_string())))
        }
        OID_JSONB => {
            let decoded_value = String::from_value(value, spanner_type).map_err(|e| {
                anyhow::anyhow!("Failed to convert Spanner value to String for JSON: {}", e)
            })?;
            Ok(Some(Bytes::from(decoded_value)))
        }
        OID_ARRAY_BOOL
        | OID_ARRAY_INT8
        | OID_ARRAY_INT4
        | OID_ARRAY_TEXT
        | OID_ARRAY_FLOAT8
        | OID_ARRAY_FLOAT4
        | OID_ARRAY_NUMERIC
        | OID_ARRAY_DATE
        | OID_ARRAY_TIMESTAMPTZ
        | OID_ARRAY_JSONB => {
            let elem_oid = match oid {
                OID_ARRAY_BOOL => OID_BOOL,
                OID_ARRAY_INT8 => OID_INT8,
                OID_ARRAY_INT4 => OID_INT4,
                OID_ARRAY_TEXT => OID_TEXT,
                OID_ARRAY_FLOAT8 => OID_FLOAT8,
                OID_ARRAY_FLOAT4 => OID_FLOAT4,
                OID_ARRAY_NUMERIC => OID_NUMERIC,
                OID_ARRAY_DATE => OID_DATE,
                OID_ARRAY_TIMESTAMPTZ => OID_TIMESTAMPTZ,
                OID_ARRAY_JSONB => OID_JSONB,
                _ => OID_TEXT,
            };
            spanner_array_to_pgwire_text(value, spanner_type, elem_oid)
        }
        _ => {
            let decoded_value = String::from_value(value, spanner_type).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to convert Spanner value to String (fallback): {}",
                    e
                )
            })?;
            Ok(Some(Bytes::from(decoded_value)))
        }
    }
}

/// Maps a Spanner Type to its corresponding PostgreSQL Type OID.
pub fn map_spanner_type_to_oid(spanner_type: &SpannerType) -> Result<u32, anyhow::Error> {
    match spanner_type.code() {
        SpannerTypeCode::Bool => Ok(OID_BOOL),
        SpannerTypeCode::Int64 => Ok(OID_INT8),
        SpannerTypeCode::Float64 => Ok(OID_FLOAT8),
        SpannerTypeCode::Float32 => Ok(OID_FLOAT4),
        SpannerTypeCode::String => Ok(OID_TEXT),
        SpannerTypeCode::Bytes => Ok(OID_BYTEA),
        SpannerTypeCode::Timestamp => Ok(OID_TIMESTAMPTZ),
        SpannerTypeCode::Date => Ok(OID_DATE),
        SpannerTypeCode::Numeric => Ok(OID_NUMERIC),
        SpannerTypeCode::Json => Ok(OID_JSONB),
        SpannerTypeCode::Array => {
            if let Some(elem_type) = spanner_type.array_element_type() {
                let elem_oid = map_spanner_type_to_oid(&elem_type)?;
                match elem_oid {
                    OID_BOOL => Ok(OID_ARRAY_BOOL),
                    OID_INT8 => Ok(OID_ARRAY_INT8),
                    OID_INT4 => Ok(OID_ARRAY_INT4),
                    OID_TEXT => Ok(OID_ARRAY_TEXT),
                    OID_FLOAT8 => Ok(OID_ARRAY_FLOAT8),
                    OID_FLOAT4 => Ok(OID_ARRAY_FLOAT4),
                    OID_NUMERIC => Ok(OID_ARRAY_NUMERIC),
                    OID_TIMESTAMP | OID_TIMESTAMPTZ => Ok(OID_ARRAY_TIMESTAMPTZ),
                    OID_DATE => Ok(OID_ARRAY_DATE),
                    OID_JSONB => Ok(OID_ARRAY_JSONB),
                    _ => Ok(OID_ARRAY_TEXT),
                }
            } else {
                Ok(OID_ARRAY_TEXT)
            }
        }
        _ => Ok(OID_TEXT),
    }
}

/// Maps a PostgreSQL Type OID to its corresponding Spanner Type.
pub fn map_oid_to_spanner_type(oid: u32) -> Option<SpannerType> {
    match oid {
        OID_BOOL => Some(create_spanner_type(SpannerTypeCode::Bool)),
        OID_INT8 | OID_INT4 => Some(create_spanner_type(SpannerTypeCode::Int64)),
        OID_TEXT | OID_VARCHAR => Some(create_spanner_type(SpannerTypeCode::String)),
        OID_BYTEA => Some(create_spanner_type(SpannerTypeCode::Bytes)),
        OID_FLOAT8 => Some(create_spanner_type(SpannerTypeCode::Float64)),
        OID_FLOAT4 => Some(create_spanner_type(SpannerTypeCode::Float32)),
        OID_NUMERIC => Some(create_spanner_type_with_annotation(
            SpannerTypeCode::Numeric,
            google_cloud_spanner::model::TypeAnnotationCode::PgNumeric,
        )),
        OID_TIMESTAMP | OID_TIMESTAMPTZ => Some(create_spanner_type(SpannerTypeCode::Timestamp)),
        OID_DATE => Some(create_spanner_type(SpannerTypeCode::Date)),
        OID_JSONB => Some(create_spanner_type_with_annotation(
            SpannerTypeCode::Json,
            google_cloud_spanner::model::TypeAnnotationCode::PgJsonb,
        )),
        // Arrays
        OID_ARRAY_BOOL => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::Bool,
        ))),
        OID_ARRAY_INT8 | OID_ARRAY_INT4 => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::Int64,
        ))),
        OID_ARRAY_TEXT => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::String,
        ))),
        OID_ARRAY_FLOAT8 => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::Float64,
        ))),
        OID_ARRAY_FLOAT4 => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::Float32,
        ))),
        OID_ARRAY_NUMERIC => Some(create_spanner_array_type(
            create_spanner_type_with_annotation(
                SpannerTypeCode::Numeric,
                google_cloud_spanner::model::TypeAnnotationCode::PgNumeric,
            ),
        )),
        OID_ARRAY_TIMESTAMP | OID_ARRAY_TIMESTAMPTZ => Some(create_spanner_array_type(
            create_spanner_type(SpannerTypeCode::Timestamp),
        )),
        OID_ARRAY_DATE => Some(create_spanner_array_type(create_spanner_type(
            SpannerTypeCode::Date,
        ))),
        OID_ARRAY_JSONB => Some(create_spanner_array_type(
            create_spanner_type_with_annotation(
                SpannerTypeCode::Json,
                google_cloud_spanner::model::TypeAnnotationCode::PgJsonb,
            ),
        )),
        _ => None,
    }
}

fn create_spanner_type(code: SpannerTypeCode) -> SpannerType {
    let mut proto = google_cloud_spanner::model::Type::default();
    proto.code = code.into();
    SpannerType::from(proto)
}

fn create_spanner_type_with_annotation(
    code: SpannerTypeCode,
    annotation: google_cloud_spanner::model::TypeAnnotationCode,
) -> SpannerType {
    let mut proto = google_cloud_spanner::model::Type::default();
    proto.code = code.into();
    proto.type_annotation = annotation;
    SpannerType::from(proto)
}

fn create_spanner_array_type(elem_type: SpannerType) -> SpannerType {
    let mut proto = google_cloud_spanner::model::Type::default();
    proto.code = SpannerTypeCode::Array.into();
    proto.array_element_type = Some(Box::new(elem_type.into()));
    SpannerType::from(proto)
}

/// Converts a PG wire byte slice (text or binary format) for a given OID into a Spanner Value.
pub fn pgwire_bytes_to_spanner_value(
    bytes: &[u8],
    oid: u32,
    format_code: i16,
) -> Result<SpannerValue, anyhow::Error> {
    match oid {
        OID_ARRAY_BOOL
        | OID_ARRAY_INT8
        | OID_ARRAY_INT4
        | OID_ARRAY_TEXT
        | OID_ARRAY_FLOAT8
        | OID_ARRAY_FLOAT4
        | OID_ARRAY_NUMERIC
        | OID_ARRAY_DATE
        | OID_ARRAY_TIMESTAMPTZ
        | OID_ARRAY_JSONB => {
            let elem_oid = match oid {
                OID_ARRAY_BOOL => OID_BOOL,
                OID_ARRAY_INT8 => OID_INT8,
                OID_ARRAY_INT4 => OID_INT4,
                OID_ARRAY_TEXT => OID_TEXT,
                OID_ARRAY_FLOAT8 => OID_FLOAT8,
                OID_ARRAY_FLOAT4 => OID_FLOAT4,
                OID_ARRAY_NUMERIC => OID_NUMERIC,
                OID_ARRAY_DATE => OID_DATE,
                OID_ARRAY_TIMESTAMPTZ => OID_TIMESTAMPTZ,
                OID_ARRAY_JSONB => OID_JSONB,
                _ => OID_TEXT,
            };
            if format_code == 1 {
                pgwire_array_binary_to_spanner_value(bytes, elem_oid)
            } else {
                pgwire_array_text_to_spanner_value(bytes, elem_oid)
            }
        }
        _ => {
            if format_code == 1 {
                pgwire_binary_to_spanner_value(bytes, oid)
            } else {
                pgwire_text_to_spanner_value(bytes, oid)
            }
        }
    }
}

fn pgwire_text_to_spanner_value(bytes: &[u8], oid: u32) -> Result<SpannerValue, anyhow::Error> {
    let s = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to parse UTF-8 string parameter: {}", e))?;
    match oid {
        OID_BOOL => {
            let b = parse_pg_bool(s)?;
            Ok(b.to_value())
        }
        OID_INT8 | OID_INT4 => {
            let i = s
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Failed to parse i64 parameter '{}': {}", s, e))?;
            Ok(i.to_value())
        }
        OID_TEXT | OID_VARCHAR => Ok(s.to_string().to_value()),
        OID_FLOAT4 => {
            let f = s
                .parse::<f32>()
                .map_err(|e| anyhow::anyhow!("Failed to parse f32 parameter '{}': {}", s, e))?;
            Ok(f.to_value())
        }
        OID_FLOAT8 => {
            let f = s
                .parse::<f64>()
                .map_err(|e| anyhow::anyhow!("Failed to parse f64 parameter '{}': {}", s, e))?;
            Ok(f.to_value())
        }
        OID_NUMERIC => {
            let d = s
                .parse::<Decimal>()
                .map_err(|e| anyhow::anyhow!("Failed to parse Decimal parameter '{}': {}", s, e))?;
            Ok(d.to_value())
        }
        OID_JSONB => {
            let _val: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| anyhow::anyhow!("Failed to parse JSON parameter '{}': {}", s, e))?;
            Ok(s.to_string().to_value())
        }
        _ => Ok(s.to_string().to_value()),
    }
}

fn pgwire_binary_to_spanner_value(bytes: &[u8], oid: u32) -> Result<SpannerValue, anyhow::Error> {
    match oid {
        OID_BOOL => {
            if bytes.is_empty() {
                return Err(anyhow::anyhow!("invalid length for bool parameter"));
            }
            Ok((bytes[0] != 0).to_value())
        }
        OID_INT8 => {
            if bytes.len() < 8 {
                return Err(anyhow::anyhow!("insufficient bytes for int8 parameter"));
            }
            let i = i64::from_be_bytes(bytes[0..8].try_into()?);
            Ok(i.to_value())
        }
        OID_INT4 => {
            if bytes.len() < 4 {
                return Err(anyhow::anyhow!("insufficient bytes for int4 parameter"));
            }
            let i = i32::from_be_bytes(bytes[0..4].try_into()?);
            Ok((i as i64).to_value())
        }
        OID_TEXT | OID_VARCHAR => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| anyhow::anyhow!("Failed to parse UTF-8 string parameter: {}", e))?;
            Ok(s.to_string().to_value())
        }
        OID_FLOAT4 => {
            if bytes.len() < 4 {
                return Err(anyhow::anyhow!("insufficient bytes for float4 parameter"));
            }
            let f = f32::from_be_bytes(bytes[0..4].try_into()?);
            Ok(f.to_value())
        }
        OID_FLOAT8 => {
            if bytes.len() < 8 {
                return Err(anyhow::anyhow!("insufficient bytes for float8 parameter"));
            }
            let f = f64::from_be_bytes(bytes[0..8].try_into()?);
            Ok(f.to_value())
        }
        OID_BYTEA => {
            let v = bytes.to_vec();
            Ok(v.to_value())
        }
        OID_JSONB => {
            if bytes.is_empty() {
                return Err(anyhow::anyhow!("empty JSON parameter"));
            }
            let version = bytes[0];
            if version != 1 {
                return Err(anyhow::anyhow!(
                    "unsupported JSONB binary format version: {}",
                    version
                ));
            }
            let s = std::str::from_utf8(&bytes[1..])
                .map_err(|e| anyhow::anyhow!("Failed to parse JSONB binary payload: {}", e))?;
            let _val: serde_json::Value = serde_json::from_str(s)
                .map_err(|e| anyhow::anyhow!("Failed to parse JSONB binary payload: {}", e))?;
            Ok(s.to_string().to_value())
        }
        _ => Err(anyhow::anyhow!(
            "unsupported binary decoding for PG OID: {}",
            oid
        )),
    }
}

fn pgwire_array_text_to_spanner_value(
    bytes: &[u8],
    elem_oid: u32,
) -> Result<SpannerValue, anyhow::Error> {
    let s = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in array parameter: {}", e))?;
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return Err(anyhow::anyhow!(
            "invalid array format, must start with '{{' and end with '}}'"
        ));
    }
    let content = &s[1..s.len() - 1];
    if content.trim().is_empty() {
        return Ok(Vec::<SpannerValue>::new().to_value());
    }

    let mut elements = Vec::new();
    let mut chars = content.chars().peekable();
    while chars.peek().is_some() {
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }
        if chars.peek().is_none() {
            break;
        }

        let mut val_str = String::new();
        let mut is_quoted = false;
        if chars.peek() == Some(&'"') {
            is_quoted = true;
            chars.next();
            while let Some(c) = chars.next() {
                if c == '"' {
                    break;
                } else if c == '\\' {
                    if let Some(escaped) = chars.next() {
                        val_str.push(escaped);
                    }
                } else {
                    val_str.push(c);
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                val_str.push(c);
                chars.next();
            }
        }

        let val_str_trimmed = val_str.trim();
        if !is_quoted && (val_str_trimmed.eq_ignore_ascii_case("null")) {
            let null_value = match elem_oid {
                OID_BOOL => None::<bool>.to_value(),
                OID_INT8 | OID_INT4 => None::<i64>.to_value(),
                OID_FLOAT8 => None::<f64>.to_value(),
                OID_FLOAT4 => None::<f32>.to_value(),
                OID_TEXT | OID_VARCHAR => None::<String>.to_value(),
                OID_BYTEA => None::<Vec<u8>>.to_value(),
                OID_NUMERIC => None::<Decimal>.to_value(),
                OID_JSONB => None::<String>.to_value(),
                _ => None::<String>.to_value(),
            };
            elements.push(null_value);
        } else {
            let val = pgwire_text_to_spanner_value(val_str_trimmed.as_bytes(), elem_oid)?;
            elements.push(val);
        }

        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    Ok(elements.to_value())
}

fn pgwire_array_binary_to_spanner_value(
    bytes: &[u8],
    elem_oid: u32,
) -> Result<SpannerValue, anyhow::Error> {
    if bytes.len() < 12 {
        return Err(anyhow::anyhow!("invalid array binary header length"));
    }
    let ndim = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if ndim == 0 {
        return Ok(Vec::<SpannerValue>::new().to_value());
    }
    if ndim != 1 {
        return Err(anyhow::anyhow!("only 1-dimensional arrays are supported"));
    }
    let _flags = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    let _elemtype = u32::from_be_bytes(bytes[8..12].try_into().unwrap());

    let mut cursor = 12;
    if bytes.len() < cursor + 8 {
        return Err(anyhow::anyhow!("invalid array binary dimension info"));
    }
    let count = i32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 8;

    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        if bytes.len() < cursor + 4 {
            return Err(anyhow::anyhow!("invalid array binary element length"));
        }
        let len = i32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;

        if len == -1 {
            let null_value = match elem_oid {
                OID_BOOL => None::<bool>.to_value(),
                OID_INT8 | OID_INT4 => None::<i64>.to_value(),
                OID_FLOAT8 => None::<f64>.to_value(),
                OID_FLOAT4 => None::<f32>.to_value(),
                OID_TEXT | OID_VARCHAR => None::<String>.to_value(),
                OID_BYTEA => None::<Vec<u8>>.to_value(),
                OID_NUMERIC => None::<Decimal>.to_value(),
                OID_JSONB => None::<String>.to_value(),
                _ => None::<String>.to_value(),
            };
            elements.push(null_value);
        } else {
            let len_usize = len as usize;
            if bytes.len() < cursor + len_usize {
                return Err(anyhow::anyhow!("array binary element payload too short"));
            }
            let elem_bytes = &bytes[cursor..cursor + len_usize];
            cursor += len_usize;

            let val = pgwire_binary_to_spanner_value(elem_bytes, elem_oid)?;
            elements.push(val);
        }
    }

    Ok(elements.to_value())
}

fn spanner_array_to_pgwire_text(
    value: &SpannerValue,
    spanner_type: &SpannerType,
    elem_oid: u32,
) -> Result<Option<Bytes>, anyhow::Error> {
    let elements = Vec::<SpannerValue>::from_value(value, spanner_type)
        .map_err(|e| anyhow::anyhow!("Failed to convert Spanner array value: {}", e))?;

    let elem_type = spanner_type
        .array_element_type()
        .ok_or_else(|| anyhow::anyhow!("Expected array element type in {:?}", spanner_type))?;

    let mut result_str = String::from("{");
    for (i, elem) in elements.iter().enumerate() {
        if i > 0 {
            result_str.push(',');
        }
        if elem.kind() == SpannerValueKind::Null {
            result_str.push_str("NULL");
        } else {
            let elem_bytes = spanner_value_to_pgwire_text(elem, &elem_type, elem_oid)?;
            if let Some(bytes) = elem_bytes {
                let s = std::str::from_utf8(&bytes)?;
                if s.contains(',')
                    || s.contains('{')
                    || s.contains('}')
                    || s.contains(' ')
                    || s.contains('"')
                {
                    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                    result_str.push('"');
                    result_str.push_str(&escaped);
                    result_str.push('"');
                } else {
                    result_str.push_str(s);
                }
            } else {
                result_str.push_str("NULL");
            }
        }
    }
    result_str.push('}');
    Ok(Some(Bytes::from(result_str)))
}

fn spanner_array_to_pgwire_binary(
    value: &SpannerValue,
    spanner_type: &SpannerType,
    elem_oid: u32,
) -> Result<Option<Bytes>, anyhow::Error> {
    let elements = Vec::<SpannerValue>::from_value(value, spanner_type)
        .map_err(|e| anyhow::anyhow!("Failed to convert Spanner array value: {}", e))?;

    let mut payload = BytesMut::new();
    payload.put_i32(1); // ndim = 1
    payload.put_i32(0); // flags = 0
    payload.put_u32(elem_oid); // elemtype
    payload.put_i32(elements.len() as i32); // dim count
    payload.put_i32(1); // lbound = 1

    let elem_type = spanner_type
        .array_element_type()
        .ok_or_else(|| anyhow::anyhow!("Expected array element type in {:?}", spanner_type))?;

    for elem in &elements {
        if elem.kind() == SpannerValueKind::Null {
            payload.put_i32(-1);
        } else {
            let elem_bytes = spanner_value_to_pgwire_binary(elem, &elem_type, elem_oid)?;
            if let Some(bytes) = elem_bytes {
                payload.put_i32(bytes.len() as i32);
                payload.put_slice(&bytes);
            } else {
                payload.put_i32(-1);
            }
        }
    }

    Ok(Some(payload.freeze()))
}

fn parse_pg_bool(s: &str) -> Result<bool, anyhow::Error> {
    let trimmed = s.trim().to_lowercase();
    match trimmed.as_str() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(true),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(false),
        _ => Err(anyhow::anyhow!(
            "invalid input syntax for type boolean: \"{}\"",
            s
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_pg_bool() {
        assert_eq!(parse_pg_bool("t").unwrap(), true);
        assert_eq!(parse_pg_bool("T").unwrap(), true);
        assert_eq!(parse_pg_bool("true").unwrap(), true);
        assert_eq!(parse_pg_bool("TrUe").unwrap(), true);
        assert_eq!(parse_pg_bool("yes").unwrap(), true);
        assert_eq!(parse_pg_bool("on").unwrap(), true);
        assert_eq!(parse_pg_bool("1").unwrap(), true);
        assert_eq!(parse_pg_bool(" y ").unwrap(), true); // whitespace trimming

        assert_eq!(parse_pg_bool("f").unwrap(), false);
        assert_eq!(parse_pg_bool("F").unwrap(), false);
        assert_eq!(parse_pg_bool("false").unwrap(), false);
        assert_eq!(parse_pg_bool("no").unwrap(), false);
        assert_eq!(parse_pg_bool("of").unwrap(), false);
        assert_eq!(parse_pg_bool("off").unwrap(), false);
        assert_eq!(parse_pg_bool("0").unwrap(), false);

        assert!(parse_pg_bool("o").is_err());
        assert!(parse_pg_bool("invalid").is_err());
    }

    #[test]
    fn test_type_mappings() {
        // --- Spanner Type to OID ---
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Bool)).unwrap(),
            OID_BOOL
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Int64)).unwrap(),
            OID_INT8
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Float32)).unwrap(),
            OID_FLOAT4
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Float64)).unwrap(),
            OID_FLOAT8
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::String)).unwrap(),
            OID_TEXT
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Bytes)).unwrap(),
            OID_BYTEA
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Timestamp)).unwrap(),
            OID_TIMESTAMPTZ
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Date)).unwrap(),
            OID_DATE
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Numeric)).unwrap(),
            OID_NUMERIC
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_type(SpannerTypeCode::Json)).unwrap(),
            OID_JSONB
        );

        // Arrays
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Bool
            )))
            .unwrap(),
            OID_ARRAY_BOOL
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Int64
            )))
            .unwrap(),
            OID_ARRAY_INT8
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Float32
            )))
            .unwrap(),
            OID_ARRAY_FLOAT4
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Float64
            )))
            .unwrap(),
            OID_ARRAY_FLOAT8
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::String
            )))
            .unwrap(),
            OID_ARRAY_TEXT
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Bytes
            )))
            .unwrap(),
            OID_ARRAY_TEXT
        ); // default fallback
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Timestamp
            )))
            .unwrap(),
            OID_ARRAY_TIMESTAMPTZ
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(create_spanner_type(
                SpannerTypeCode::Date
            )))
            .unwrap(),
            OID_ARRAY_DATE
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(
                create_spanner_type_with_annotation(
                    SpannerTypeCode::Numeric,
                    google_cloud_spanner::model::TypeAnnotationCode::PgNumeric
                )
            ))
            .unwrap(),
            OID_ARRAY_NUMERIC
        );
        assert_eq!(
            map_spanner_type_to_oid(&create_spanner_array_type(
                create_spanner_type_with_annotation(
                    SpannerTypeCode::Json,
                    google_cloud_spanner::model::TypeAnnotationCode::PgJsonb
                )
            ))
            .unwrap(),
            OID_ARRAY_JSONB
        );

        // --- OID to Spanner Type ---
        assert_eq!(
            map_oid_to_spanner_type(OID_BOOL).unwrap(),
            create_spanner_type(SpannerTypeCode::Bool)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_INT8).unwrap(),
            create_spanner_type(SpannerTypeCode::Int64)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_INT4).unwrap(),
            create_spanner_type(SpannerTypeCode::Int64)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_TEXT).unwrap(),
            create_spanner_type(SpannerTypeCode::String)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_VARCHAR).unwrap(),
            create_spanner_type(SpannerTypeCode::String)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_BYTEA).unwrap(),
            create_spanner_type(SpannerTypeCode::Bytes)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_FLOAT4).unwrap(),
            create_spanner_type(SpannerTypeCode::Float32)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_FLOAT8).unwrap(),
            create_spanner_type(SpannerTypeCode::Float64)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_NUMERIC).unwrap(),
            create_spanner_type_with_annotation(
                SpannerTypeCode::Numeric,
                google_cloud_spanner::model::TypeAnnotationCode::PgNumeric
            )
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_TIMESTAMP).unwrap(),
            create_spanner_type(SpannerTypeCode::Timestamp)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_TIMESTAMPTZ).unwrap(),
            create_spanner_type(SpannerTypeCode::Timestamp)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_DATE).unwrap(),
            create_spanner_type(SpannerTypeCode::Date)
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_JSONB).unwrap(),
            create_spanner_type_with_annotation(
                SpannerTypeCode::Json,
                google_cloud_spanner::model::TypeAnnotationCode::PgJsonb
            )
        );

        // Arrays OID to Spanner Type
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_BOOL).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Bool))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_INT8).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Int64))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_INT4).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Int64))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_TEXT).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::String))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_FLOAT4).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Float32))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_FLOAT8).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Float64))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_NUMERIC).unwrap(),
            create_spanner_array_type(create_spanner_type_with_annotation(
                SpannerTypeCode::Numeric,
                google_cloud_spanner::model::TypeAnnotationCode::PgNumeric
            ))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_TIMESTAMP).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Timestamp))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_TIMESTAMPTZ).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Timestamp))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_DATE).unwrap(),
            create_spanner_array_type(create_spanner_type(SpannerTypeCode::Date))
        );
        assert_eq!(
            map_oid_to_spanner_type(OID_ARRAY_JSONB).unwrap(),
            create_spanner_array_type(create_spanner_type_with_annotation(
                SpannerTypeCode::Json,
                google_cloud_spanner::model::TypeAnnotationCode::PgJsonb
            ))
        );

        assert!(map_oid_to_spanner_type(9999).is_none());
    }

    #[test]
    fn test_pgwire_bytes_to_spanner_value_text() {
        // Bool
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"true", OID_BOOL, 0).unwrap(),
            true.to_value()
        );
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"f", OID_BOOL, 0).unwrap(),
            false.to_value()
        );

        // Integers
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"42", OID_INT8, 0).unwrap(),
            42i64.to_value()
        );
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"99999", OID_INT4, 0).unwrap(),
            99999i64.to_value()
        );

        // Strings
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"hello world", OID_TEXT, 0).unwrap(),
            "hello world".to_value()
        );

        // Floats
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"12.34", OID_FLOAT8, 0).unwrap(),
            12.34f64.to_value()
        );

        // Numerics
        let d = Decimal::from_str("99.99").unwrap();
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"99.99", OID_NUMERIC, 0).unwrap(),
            d.to_value()
        );
    }

    #[test]
    fn test_pgwire_bytes_to_spanner_value_binary() {
        // Bool
        assert_eq!(
            pgwire_bytes_to_spanner_value(&[1], OID_BOOL, 1).unwrap(),
            true.to_value()
        );
        assert_eq!(
            pgwire_bytes_to_spanner_value(&[0], OID_BOOL, 1).unwrap(),
            false.to_value()
        );

        // Integers
        assert_eq!(
            pgwire_bytes_to_spanner_value(&42i64.to_be_bytes(), OID_INT8, 1).unwrap(),
            42i64.to_value()
        );
        assert_eq!(
            pgwire_bytes_to_spanner_value(&999i32.to_be_bytes(), OID_INT4, 1).unwrap(),
            999i64.to_value()
        );

        // Floats
        assert_eq!(
            pgwire_bytes_to_spanner_value(&12.34f64.to_be_bytes(), OID_FLOAT8, 1).unwrap(),
            12.34f64.to_value()
        );

        // Bytes
        assert_eq!(
            pgwire_bytes_to_spanner_value(b"hello", OID_BYTEA, 1).unwrap(),
            b"hello".to_vec().to_value()
        );
    }

    #[test]
    fn test_spanner_value_to_pgwire_bytes_text() {
        let bool_type = create_spanner_type(SpannerTypeCode::Bool);
        let int_type = create_spanner_type(SpannerTypeCode::Int64);
        let float_type = create_spanner_type(SpannerTypeCode::Float64);
        let string_type = create_spanner_type(SpannerTypeCode::String);
        let numeric_type = create_spanner_type(SpannerTypeCode::Numeric);
        let bytes_type = create_spanner_type(SpannerTypeCode::Bytes);

        // Bool
        assert_eq!(
            spanner_value_to_pgwire_bytes(&true.to_value(), &bool_type, OID_BOOL, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("t")
        );
        assert_eq!(
            spanner_value_to_pgwire_bytes(&false.to_value(), &bool_type, OID_BOOL, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("f")
        );

        // Integers
        assert_eq!(
            spanner_value_to_pgwire_bytes(&42i64.to_value(), &int_type, OID_INT8, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("42")
        );

        // Floats
        assert_eq!(
            spanner_value_to_pgwire_bytes(&12.34f64.to_value(), &float_type, OID_FLOAT8, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("12.34")
        );

        // Strings
        assert_eq!(
            spanner_value_to_pgwire_bytes(&"hello".to_value(), &string_type, OID_TEXT, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("hello")
        );

        // Numerics
        let d = Decimal::from_str("99.99").unwrap();
        assert_eq!(
            spanner_value_to_pgwire_bytes(&d.to_value(), &numeric_type, OID_NUMERIC, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("99.99")
        );

        // Bytes (hex encoded in text format)
        assert_eq!(
            spanner_value_to_pgwire_bytes(&b"hello".to_vec().to_value(), &bytes_type, OID_BYTEA, 0)
                .unwrap()
                .unwrap(),
            Bytes::from("\\x68656c6c6f")
        );
    }

    #[test]
    fn test_spanner_value_to_pgwire_bytes_binary() {
        let bool_type = create_spanner_type(SpannerTypeCode::Bool);
        let int_type = create_spanner_type(SpannerTypeCode::Int64);
        let float_type = create_spanner_type(SpannerTypeCode::Float64);
        let string_type = create_spanner_type(SpannerTypeCode::String);
        let bytes_type = create_spanner_type(SpannerTypeCode::Bytes);

        // Bool
        assert_eq!(
            spanner_value_to_pgwire_bytes(&true.to_value(), &bool_type, OID_BOOL, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(vec![1])
        );
        assert_eq!(
            spanner_value_to_pgwire_bytes(&false.to_value(), &bool_type, OID_BOOL, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(vec![0])
        );

        // Integers
        assert_eq!(
            spanner_value_to_pgwire_bytes(&42i64.to_value(), &int_type, OID_INT8, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(42i64.to_be_bytes().to_vec())
        );
        assert_eq!(
            spanner_value_to_pgwire_bytes(&42i64.to_value(), &int_type, OID_INT4, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(42i32.to_be_bytes().to_vec())
        );

        // Floats
        assert_eq!(
            spanner_value_to_pgwire_bytes(&12.34f64.to_value(), &float_type, OID_FLOAT8, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(12.34f64.to_be_bytes().to_vec())
        );

        // Strings
        assert_eq!(
            spanner_value_to_pgwire_bytes(&"hello".to_value(), &string_type, OID_TEXT, 1)
                .unwrap()
                .unwrap(),
            Bytes::from("hello")
        );

        // Bytes
        assert_eq!(
            spanner_value_to_pgwire_bytes(&b"hello".to_vec().to_value(), &bytes_type, OID_BYTEA, 1)
                .unwrap()
                .unwrap(),
            Bytes::from(b"hello".to_vec())
        );
    }
}
