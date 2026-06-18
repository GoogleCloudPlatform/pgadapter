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

use crate::backend::{BackendColumn, BackendResponse, QueryBackend, QueryDescription, RowReader};
use bytes::Bytes;

/// A mock connection query backend returning hardcoded in-memory rows.
///
/// NOTE: This implementation is strictly for testing and local protocol prototyping
/// purposes. In production contexts, use `SpannerBackend` instead.
pub struct MockBackend {
    transaction_status: u8,
}

impl MockBackend {
    /// Creates a new `MockBackend` instance starting in the Idle state.
    pub fn new() -> Self {
        Self {
            transaction_status: b'I',
        }
    }
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueryBackend for MockBackend {
    async fn execute_statement(
        &mut self,
        sql: String,
        _param_types: Vec<u32>,
        _param_formats: Vec<i16>,
        _param_values: Vec<Option<Bytes>>,
        _result_formats: Vec<i16>,
    ) -> Result<BackendResponse, anyhow::Error> {
        let sql_trimmed = sql.trim().to_uppercase();
        if sql_trimmed.starts_with("SELECT_FAIL") || sql_trimmed.starts_with("FAIL") {
            return Err(anyhow::anyhow!("mock query execution error"));
        }
        if sql_trimmed.starts_with("SELECT 1") {
            let format_code = if _result_formats.is_empty() {
                0
            } else {
                _result_formats[0]
            };
            let val_bytes = if format_code == 1 {
                Bytes::from(1i32.to_be_bytes().to_vec())
            } else {
                Bytes::from("1")
            };
            Ok(BackendResponse::ResultSet {
                tag: "SELECT".to_string(),
                columns: vec![BackendColumn {
                    name: "?column?".to_string(),
                    type_oid: 23, // INT4
                    type_size: 4,
                    format_code,
                }],
                rows: Box::new(MockRowReader {
                    rows: vec![vec![Some(val_bytes)]].into_iter(),
                }),
            })
        } else if sql_trimmed.starts_with("SELECT") {
            let format_id = if _result_formats.is_empty() {
                0
            } else if _result_formats.len() == 1 {
                _result_formats[0]
            } else {
                *_result_formats.get(0).unwrap_or(&0)
            };
            let format_name = if _result_formats.is_empty() {
                0
            } else if _result_formats.len() == 1 {
                _result_formats[0]
            } else {
                *_result_formats.get(1).unwrap_or(&0)
            };

            let id1_bytes = if format_id == 1 {
                Bytes::from(1i32.to_be_bytes().to_vec())
            } else {
                Bytes::from("1")
            };
            let id2_bytes = if format_id == 1 {
                Bytes::from(2i32.to_be_bytes().to_vec())
            } else {
                Bytes::from("2")
            };

            let name1_bytes = Bytes::from("Alice");
            let name2_bytes = Bytes::from("Bob");

            Ok(BackendResponse::ResultSet {
                tag: "SELECT".to_string(),
                columns: vec![
                    BackendColumn {
                        name: "id".to_string(),
                        type_oid: 23, // INT4
                        type_size: 4,
                        format_code: format_id,
                    },
                    BackendColumn {
                        name: "name".to_string(),
                        type_oid: 1043, // VARCHAR
                        type_size: -1,
                        format_code: format_name,
                    },
                ],
                rows: Box::new(MockRowReader {
                    rows: vec![
                        vec![Some(id1_bytes), Some(name1_bytes)],
                        vec![Some(id2_bytes), Some(name2_bytes)],
                    ]
                    .into_iter(),
                }),
            })
        } else {
            let tag = if sql_trimmed.starts_with("BEGIN") {
                self.transaction_status = b'T';
                "BEGIN"
            } else if sql_trimmed.starts_with("COMMIT") {
                self.transaction_status = b'I';
                "COMMIT"
            } else if sql_trimmed.starts_with("ROLLBACK") {
                self.transaction_status = b'I';
                "ROLLBACK"
            } else if sql_trimmed.starts_with("INSERT") {
                return Ok(BackendResponse::UpdateComplete {
                    tag_prefix: "INSERT".to_string(),
                    count: 1,
                });
            } else if sql_trimmed.starts_with("UPDATE") {
                return Ok(BackendResponse::UpdateComplete {
                    tag_prefix: "UPDATE".to_string(),
                    count: 1,
                });
            } else if sql_trimmed.starts_with("DELETE") {
                return Ok(BackendResponse::UpdateComplete {
                    tag_prefix: "DELETE".to_string(),
                    count: 1,
                });
            } else {
                "SELECT 0"
            };
            Ok(BackendResponse::CommandComplete(tag.to_string()))
        }
    }

    fn transaction_status(&self) -> u8 {
        self.transaction_status
    }

    async fn describe_query(&mut self, sql: String) -> Result<QueryDescription, anyhow::Error> {
        let sql_trimmed = sql.trim().to_uppercase();
        let param_types = if sql_trimmed.contains("$1") {
            vec![23] // INT4
        } else {
            vec![]
        };

        let columns = if sql_trimmed.starts_with("SELECT 1") {
            vec![BackendColumn {
                name: "?column?".to_string(),
                type_oid: 23, // INT4
                type_size: 4,
                format_code: 0,
            }]
        } else if sql_trimmed.starts_with("SELECT") {
            vec![
                BackendColumn {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_size: 4,
                    format_code: 0,
                },
                BackendColumn {
                    name: "name".to_string(),
                    type_oid: 1043,
                    type_size: -1,
                    format_code: 0,
                },
            ]
        } else {
            vec![]
        };

        Ok(QueryDescription {
            param_types,
            columns,
        })
    }
    async fn execute_batch_dml(
        &mut self,
        statements: Vec<(String, Vec<u32>, Vec<i16>, Vec<Option<Bytes>>)>,
    ) -> Result<Vec<Result<i64, anyhow::Error>>, anyhow::Error> {
        let mut results = Vec::new();
        for (sql, _, _, _) in statements {
            let sql_trimmed = sql.trim().to_uppercase();
            if sql_trimmed.contains("FAIL") {
                results.push(Err(anyhow::anyhow!("mock batch statement error")));
            } else {
                results.push(Ok(1));
            }
        }
        Ok(results)
    }
}

struct MockRowReader {
    rows: std::vec::IntoIter<Vec<Option<Bytes>>>,
}

#[async_trait::async_trait]
impl RowReader for MockRowReader {
    async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, anyhow::Error> {
        Ok(self.rows.next())
    }
}
