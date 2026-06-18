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

use bytes::Bytes;

/// Represents a single column description in a query result set.
#[derive(Clone, Debug, PartialEq)]
pub struct BackendColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_size: i16,
    pub format_code: i16,
}

#[async_trait::async_trait]
pub trait RowReader: Send + Sync {
    async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, anyhow::Error>;
}

/// Represents a response from the backend query engine.
pub enum BackendResponse {
    /// Command completed successfully (e.g. for DDL, DML, etc.).
    CommandComplete(String),
    /// DML update result containing the tag prefix and number of affected rows.
    UpdateComplete { tag_prefix: String, count: i64 },
    /// Query result set.
    ResultSet {
        tag: String,
        columns: Vec<BackendColumn>,
        rows: Box<dyn RowReader>,
    },
}

impl std::fmt::Debug for BackendResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendResponse::CommandComplete(tag) => {
                f.debug_tuple("CommandComplete").field(tag).finish()
            }
            BackendResponse::UpdateComplete { tag_prefix, count } => f
                .debug_struct("UpdateComplete")
                .field("tag_prefix", tag_prefix)
                .field("count", count)
                .finish(),
            BackendResponse::ResultSet { tag, columns, .. } => f
                .debug_struct("ResultSet")
                .field("tag", tag)
                .field("columns", columns)
                .field("rows", &"<streaming row reader>")
                .finish(),
        }
    }
}

/// Represents the described parameters and columns of a query.
#[derive(Clone, Debug)]
pub struct QueryDescription {
    pub param_types: Vec<u32>,
    pub columns: Vec<BackendColumn>,
}

#[async_trait::async_trait]
pub trait QueryBackend: Send + Sync {
    /// Dynamically initializes the backend with the connection DSN.
    async fn init(&mut self, _dsn: String) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Executes a single SQL query or command string with parameters and returns its outcome.
    async fn execute_statement(
        &mut self,
        sql: String,
        param_types: Vec<u32>,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
    ) -> Result<BackendResponse, anyhow::Error>;

    /// Returns the current transaction status byte (e.g. b'I' for Idle, b'T' for In Transaction).
    fn transaction_status(&self) -> u8;

    /// Describes the parameters and output schema of a query.
    async fn describe_query(&mut self, sql: String) -> Result<QueryDescription, anyhow::Error>;

    /// Executes a batch of DML statements and returns their update counts (or error results).
    async fn execute_batch_dml(
        &mut self,
        statements: Vec<(String, Vec<u32>, Vec<i16>, Vec<Option<Bytes>>)>,
    ) -> Result<Vec<Result<i64, anyhow::Error>>, anyhow::Error>;
}
