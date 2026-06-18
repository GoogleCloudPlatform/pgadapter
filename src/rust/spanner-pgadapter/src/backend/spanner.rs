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
use crate::pgwire::command::parse_command_tag;
use crate::pgwire::types::{
    map_oid_to_spanner_type, map_spanner_type_to_oid, pgwire_bytes_to_spanner_value,
    spanner_value_to_pgwire_bytes,
};
use bytes::Bytes;
use google_cloud_spanner::connection::{Connection, ExecutionResult};
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::Value;
use tracing::info;

/// Production Spanner connection query backend.
pub struct SpannerBackend {
    conn: Option<Connection>,
    dsn: Option<String>,
}

impl SpannerBackend {
    /// Creates a new `SpannerBackend` instance.
    pub fn new() -> Self {
        Self {
            conn: None,
            dsn: None,
        }
    }
}

impl Default for SpannerBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueryBackend for SpannerBackend {
    async fn init(&mut self, dsn: String) -> Result<(), anyhow::Error> {
        info!("initializing Spanner connection with DSN: {}", dsn);
        self.dsn = Some(dsn.clone());
        let conn = Connection::connect(&dsn).await?;
        self.conn = Some(conn);
        Ok(())
    }

    async fn execute_statement(
        &mut self,
        sql: String,
        param_types: Vec<u32>,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
    ) -> Result<BackendResponse, anyhow::Error> {
        let translated_sql = crate::catalog::translate_query(&sql);
        let command_tag = parse_command_tag(&translated_sql);
        let conn = self.conn.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Spanner backend not initialized. Did startup handshake succeed?")
        })?;
        let statement =
            build_statement(translated_sql, &param_types, &param_formats, &param_values)?;
        let res = conn.execute(statement).await;
        map_execution_result(conn, res, &command_tag, result_formats).await
    }

    fn transaction_status(&self) -> u8 {
        if let Some(ref conn) = self.conn {
            if conn.state().in_transaction() {
                b'T'
            } else {
                b'I'
            }
        } else {
            b'I'
        }
    }

    async fn describe_query(&mut self, sql: String) -> Result<QueryDescription, anyhow::Error> {
        let conn = self.conn.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Spanner backend not initialized. Did startup handshake succeed?")
        })?;

        let translated_sql = crate::catalog::translate_query(&sql);
        let statement = Statement::builder(translated_sql)
            .set_query_mode(QueryMode::Plan)
            .build();

        let res = conn.execute(statement).await;
        map_describe_result(conn, res).await
    }
    async fn execute_batch_dml(
        &mut self,
        statements: Vec<(String, Vec<u32>, Vec<i16>, Vec<Option<Bytes>>)>,
    ) -> Result<Vec<Result<i64, anyhow::Error>>, anyhow::Error> {
        let conn = self.conn.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Spanner backend not initialized. Did startup handshake succeed?")
        })?;

        // 1. Start Batch DML
        conn.execute("START BATCH DML").await?;

        // 2. Execute each statement to push it to the batch
        for (sql, param_types, param_formats, param_values) in &statements {
            let statement = build_statement(sql.clone(), param_types, param_formats, param_values)?;
            conn.execute(statement).await?;
        }

        // 3. Run Batch DML
        let run_res = conn.execute("RUN BATCH").await;
        match run_res {
            Ok(ExecutionResult::BatchUpdateResult(counts)) => {
                let results = counts.into_iter().map(Ok).collect();
                Ok(results)
            }
            Ok(_) => Err(anyhow::anyhow!("expected BatchUpdateResult")),
            Err(e) => Err(e.into()),
        }
    }
}

async fn map_execution_result(
    _conn: &mut Connection,
    exec_res: Result<ExecutionResult, google_cloud_spanner::Error>,
    command_tag: &str,
    result_formats: Vec<i16>,
) -> Result<BackendResponse, anyhow::Error> {
    let exec_result = exec_res?;
    match exec_result {
        ExecutionResult::QueryResult(result_set) => {
            map_query_result(result_set, command_tag, result_formats).await
        }
        ExecutionResult::UpdateResult(count) => Ok(BackendResponse::UpdateComplete {
            tag_prefix: command_tag.to_string(),
            count,
        }),
        ExecutionResult::Success => Ok(BackendResponse::CommandComplete("SUCCESS".to_string())),
        _ => Err(anyhow::anyhow!("unexpected execution result variant")),
    }
}

fn map_columns(
    names: &[String],
    types: &[google_cloud_spanner::value::Type],
    result_formats: &[i16],
) -> Result<Vec<BackendColumn>, anyhow::Error> {
    let mut columns = Vec::new();
    for (i, (name, col_type)) in names.iter().zip(types.iter()).enumerate() {
        let type_oid = map_spanner_type_to_oid(col_type)?;
        let format_code = if result_formats.is_empty() {
            0
        } else if result_formats.len() == 1 {
            result_formats[0]
        } else {
            *result_formats.get(i).unwrap_or(&0)
        };
        columns.push(BackendColumn {
            name: name.clone(),
            type_oid,
            type_size: -1,
            format_code,
        });
    }
    Ok(columns)
}

async fn map_describe_result(
    _conn: &mut Connection,
    exec_res: Result<ExecutionResult, google_cloud_spanner::Error>,
) -> Result<QueryDescription, anyhow::Error> {
    let exec_result = exec_res?;
    match exec_result {
        ExecutionResult::QueryResult(result_set) => {
            let mut param_types = Vec::new();
            let mut columns = Vec::new();

            if let Some(metadata) = result_set.metadata() {
                let undeclared = metadata.undeclared_parameters();
                for i in 1..=undeclared.len() {
                    let name = format!("p{}", i);
                    if let Some(param_type) = undeclared.get(&name) {
                        let type_oid = map_spanner_type_to_oid(param_type)?;
                        param_types.push(type_oid);
                    } else {
                        param_types.push(0);
                    }
                }

                let names = metadata.column_names();
                let types = metadata.column_types();
                columns = map_columns(names, types, &[])?;
            }

            Ok(QueryDescription {
                param_types,
                columns,
            })
        }
        _ => Ok(QueryDescription {
            param_types: Vec::new(),
            columns: Vec::new(),
        }),
    }
}

struct ColumnFormatInfo {
    column_type: google_cloud_spanner::value::Type,
    type_oid: u32,
    format_code: i16,
}

struct SpannerRowReader {
    result_set: google_cloud_spanner::result::ResultSet,
    columns_info: Vec<ColumnFormatInfo>,
}

#[async_trait::async_trait]
impl RowReader for SpannerRowReader {
    async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, anyhow::Error> {
        if let Some(row_result) = self.result_set.next().await {
            let row = row_result?;
            let mut row_data = Vec::with_capacity(row.raw_values().len());
            for (i, info) in self.columns_info.iter().enumerate() {
                let raw_value = row
                    .raw_values()
                    .get(i)
                    .expect("raw value index must be valid");
                let bytes = spanner_value_to_pgwire_bytes(
                    raw_value,
                    &info.column_type,
                    info.type_oid,
                    info.format_code,
                )?;
                row_data.push(bytes);
            }
            Ok(Some(row_data))
        } else {
            Ok(None)
        }
    }
}

async fn map_query_result(
    result_set: Box<google_cloud_spanner::result::ResultSet>,
    command_tag: &str,
    result_formats: Vec<i16>,
) -> Result<BackendResponse, anyhow::Error> {
    let mut columns = Vec::new();
    let mut columns_info = Vec::new();
    if let Some(metadata) = result_set.metadata() {
        let names = metadata.column_names();
        let types = metadata.column_types();
        columns = map_columns(names, types, &result_formats)?;

        for (i, col_type) in types.iter().enumerate() {
            let type_oid = map_spanner_type_to_oid(col_type)?;
            let format_code = if result_formats.is_empty() {
                0
            } else if result_formats.len() == 1 {
                result_formats[0]
            } else {
                *result_formats.get(i).unwrap_or(&0)
            };
            columns_info.push(ColumnFormatInfo {
                column_type: col_type.clone(),
                type_oid,
                format_code,
            });
        }
    }

    Ok(BackendResponse::ResultSet {
        tag: command_tag.to_string(),
        columns,
        rows: Box::new(SpannerRowReader {
            result_set: *result_set,
            columns_info,
        }),
    })
}

fn build_statement(
    sql: String,
    param_types: &[u32],
    param_formats: &[i16],
    param_values: &[Option<Bytes>],
) -> Result<Statement, anyhow::Error> {
    let mut builder = Statement::builder(sql);
    for (i, (type_oid, val_bytes)) in param_types.iter().zip(param_values.iter()).enumerate() {
        let param_name = format!("p{}", i + 1);
        let format_code = if param_formats.len() == 1 {
            param_formats[0]
        } else if i < param_formats.len() {
            param_formats[i]
        } else {
            0
        };

        let spanner_type_opt = map_oid_to_spanner_type(*type_oid);

        if let Some(bytes) = val_bytes {
            let value = pgwire_bytes_to_spanner_value(bytes, *type_oid, format_code)?;
            if let Some(spanner_type) = spanner_type_opt {
                builder = builder.add_typed_param(param_name, &value, spanner_type);
            } else {
                builder = builder.add_param(param_name, &value);
            }
        } else {
            if let Some(spanner_type) = spanner_type_opt {
                builder = builder.add_typed_param(param_name, &None::<Value>, spanner_type);
            } else {
                builder = builder.add_param(param_name, &None::<Value>);
            }
        }
    }
    Ok(builder.build())
}
