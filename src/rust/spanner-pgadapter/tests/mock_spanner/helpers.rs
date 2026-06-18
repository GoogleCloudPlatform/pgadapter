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

use super::google::spanner::v1::{
    PartialResultSet, ResultSetMetadata, StructType, Type, TypeCode, struct_type::Field,
};
use prost_types::{Value as ProtoValue, value::Kind};

pub fn make_string_partial_result_set(
    columns: Vec<&str>,
    rows: Vec<Vec<&str>>,
) -> PartialResultSet {
    let fields = columns
        .into_iter()
        .map(|name| {
            let mut col_type = Type::default();
            col_type.code = TypeCode::String as i32;

            Field {
                name: name.to_string(),
                r#type: Some(col_type),
            }
        })
        .collect();

    let proto_values = rows
        .into_iter()
        .flat_map(|row_vals| {
            row_vals.into_iter().map(|val| ProtoValue {
                kind: Some(Kind::StringValue(val.to_string())),
            })
        })
        .collect();

    let mut prs = PartialResultSet::default();
    prs.metadata = Some(ResultSetMetadata {
        row_type: Some(StructType { fields }),
        transaction: None,
        undeclared_parameters: None,
    });
    prs.values = proto_values;
    prs
}

pub fn make_int64_partial_result_set(columns: Vec<&str>, rows: Vec<Vec<i64>>) -> PartialResultSet {
    let fields = columns
        .into_iter()
        .map(|name| {
            let mut col_type = Type::default();
            col_type.code = TypeCode::Int64 as i32;

            Field {
                name: name.to_string(),
                r#type: Some(col_type),
            }
        })
        .collect();

    let proto_values = rows
        .into_iter()
        .flat_map(|row_vals| {
            row_vals.into_iter().map(|val| {
                ProtoValue {
                    kind: Some(Kind::StringValue(val.to_string())), // Spanner encodes int64 as string in JSON/proto Value
                }
            })
        })
        .collect();

    let mut prs = PartialResultSet::default();
    prs.metadata = Some(ResultSetMetadata {
        row_type: Some(StructType { fields }),
        transaction: None,
        undeclared_parameters: None,
    });
    prs.values = proto_values;
    prs
}

pub fn make_all_types_partial_result_set() -> PartialResultSet {
    use base64::Engine;

    let mut fields = Vec::new();
    let mut values = Vec::new();

    let add_field = |fields: &mut Vec<Field>, name: &str, code: TypeCode| {
        let mut t = Type::default();
        t.code = code as i32;
        fields.push(Field {
            name: name.to_string(),
            r#type: Some(t),
        });
    };

    let add_array_field = |fields: &mut Vec<Field>, name: &str, elem_code: TypeCode| {
        let mut elem_type = Type::default();
        elem_type.code = elem_code as i32;
        let mut arr_type = Type::default();
        arr_type.code = TypeCode::Array as i32;
        arr_type.array_element_type = Some(Box::new(elem_type));
        fields.push(Field {
            name: name.to_string(),
            r#type: Some(arr_type),
        });
    };

    // Simple types
    add_field(&mut fields, "col_bool", TypeCode::Bool);
    values.push(ProtoValue {
        kind: Some(Kind::BoolValue(true)),
    });

    add_field(&mut fields, "col_int8", TypeCode::Int64);
    values.push(ProtoValue {
        kind: Some(Kind::StringValue("123456789".to_string())),
    });

    add_field(&mut fields, "col_int4", TypeCode::Int64);
    values.push(ProtoValue {
        kind: Some(Kind::StringValue("12345".to_string())),
    });

    add_field(&mut fields, "col_float4", TypeCode::Float32);
    values.push(ProtoValue {
        kind: Some(Kind::NumberValue(12.34f64)),
    });

    add_field(&mut fields, "col_float8", TypeCode::Float64);
    values.push(ProtoValue {
        kind: Some(Kind::NumberValue(56.78f64)),
    });

    add_field(&mut fields, "col_text", TypeCode::String);
    values.push(ProtoValue {
        kind: Some(Kind::StringValue("hello text".to_string())),
    });

    add_field(&mut fields, "col_numeric", TypeCode::Numeric);
    values.push(ProtoValue {
        kind: Some(Kind::StringValue("999.99".to_string())),
    });

    add_field(&mut fields, "col_bytea", TypeCode::Bytes);
    let base64_bytes = base64::engine::general_purpose::STANDARD.encode(b"hello bytes");
    values.push(ProtoValue {
        kind: Some(Kind::StringValue(base64_bytes)),
    });

    add_field(&mut fields, "col_jsonb", TypeCode::Json);
    values.push(ProtoValue {
        kind: Some(Kind::StringValue("{\"key\": \"value\"}".to_string())),
    });

    // Arrays
    add_array_field(&mut fields, "col_arr_bool", TypeCode::Bool);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::BoolValue(true)),
                },
                ProtoValue {
                    kind: Some(Kind::BoolValue(false)),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_int8", TypeCode::Int64);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::StringValue("100".to_string())),
                },
                ProtoValue {
                    kind: Some(Kind::StringValue("200".to_string())),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_int4", TypeCode::Int64);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::StringValue("10".to_string())),
                },
                ProtoValue {
                    kind: Some(Kind::StringValue("20".to_string())),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_text", TypeCode::String);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::StringValue("a".to_string())),
                },
                ProtoValue {
                    kind: Some(Kind::StringValue("b".to_string())),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_float4", TypeCode::Float32);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::NumberValue(1.1f64)),
                },
                ProtoValue {
                    kind: Some(Kind::NumberValue(2.2f64)),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_float8", TypeCode::Float64);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::NumberValue(3.3f64)),
                },
                ProtoValue {
                    kind: Some(Kind::NumberValue(4.4f64)),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_numeric", TypeCode::Numeric);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::StringValue("11.11".to_string())),
                },
                ProtoValue {
                    kind: Some(Kind::StringValue("22.22".to_string())),
                },
            ],
        })),
    });

    add_array_field(&mut fields, "col_arr_jsonb", TypeCode::Json);
    values.push(ProtoValue {
        kind: Some(Kind::ListValue(prost_types::ListValue {
            values: vec![
                ProtoValue {
                    kind: Some(Kind::StringValue("{\"x\": 1}".to_string())),
                },
                ProtoValue {
                    kind: Some(Kind::StringValue("{\"y\": 2}".to_string())),
                },
            ],
        })),
    });

    let mut prs = PartialResultSet::default();
    prs.metadata = Some(ResultSetMetadata {
        row_type: Some(StructType { fields }),
        transaction: None,
        undeclared_parameters: None,
    });
    prs.values = values;
    prs
}
