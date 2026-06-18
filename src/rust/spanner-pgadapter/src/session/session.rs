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

use crate::backend::{BackendColumn, QueryBackend};
use crate::session::batch::{Batch, BufferedDml};
use bytes::Bytes;
use google_cloud_spanner::connection::parser::{ClientSideCommand, StatementType};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct PreparedStatement {
    pub(crate) sql: String,
    pub(crate) param_types: Vec<u32>,
    pub(crate) statement_type: StatementType,
}

pub(crate) struct Portal {
    pub(crate) sql: String,
    pub(crate) param_types: Vec<u32>,
    pub(crate) param_formats: Vec<i16>,
    pub(crate) param_values: Vec<Option<Bytes>>,
    pub(crate) result_formats: Vec<i16>,
    pub(crate) tag: Option<String>,
    pub(crate) columns: Option<Vec<BackendColumn>>,
    pub(crate) row_reader: Option<Box<dyn crate::backend::RowReader>>,
    pub(crate) total_rows_retrieved: usize,
    pub(crate) statement_type: StatementType,
}

impl std::fmt::Debug for Portal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Portal")
            .field("sql", &self.sql)
            .field("param_types", &self.param_types)
            .field("param_formats", &self.param_formats)
            .field("result_formats", &self.result_formats)
            .field("tag", &self.tag)
            .field("has_row_reader", &self.row_reader.is_some())
            .field("total_rows_retrieved", &self.total_rows_retrieved)
            .field("statement_type", &self.statement_type)
            .finish()
    }
}

pub struct Session<B: QueryBackend> {
    pub(crate) backend: B,
    pub(crate) prepared_statements: HashMap<String, PreparedStatement>,
    pub(crate) portals: HashMap<String, Portal>,
    pub(crate) in_error_state: bool,
    pub(crate) active_batch: Option<Batch>,
    pub(crate) implicit_transaction_active: bool,
}

impl<B: QueryBackend> Session<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            in_error_state: false,
            active_batch: None,
            implicit_transaction_active: false,
        }
    }

    pub(crate) async fn maybe_begin_implicit_transaction(
        &mut self,
        statement_type: &StatementType,
        read_only: bool,
    ) -> Result<(), anyhow::Error> {
        if self.implicit_transaction_active {
            return Ok(());
        }
        if self.backend.transaction_status() != b'I' {
            return Ok(());
        }

        let needs_tx = match statement_type {
            StatementType::Query | StatementType::Update { .. } => true,
            StatementType::ClientSide(ClientSideCommand::Execute { .. }) => true,
            _ => false,
        };

        if !needs_tx {
            return Ok(());
        }

        let begin_sql = if read_only {
            "BEGIN READ ONLY"
        } else {
            "BEGIN"
        };
        tracing::info!(
            "Starting implicit transaction block internally: {}",
            begin_sql
        );
        self.backend
            .execute_statement(begin_sql.to_string(), vec![], vec![], vec![], vec![])
            .await?;
        self.implicit_transaction_active = true;
        Ok(())
    }

    pub(crate) async fn maybe_commit_implicit_transaction(&mut self) -> Result<(), anyhow::Error> {
        if self.implicit_transaction_active {
            tracing::info!("Committing implicit transaction block internally");
            self.backend
                .execute_statement("COMMIT".to_string(), vec![], vec![], vec![], vec![])
                .await?;
            self.implicit_transaction_active = false;
        }
        Ok(())
    }

    pub(crate) async fn maybe_rollback_implicit_transaction(
        &mut self,
    ) -> Result<(), anyhow::Error> {
        if self.implicit_transaction_active {
            tracing::info!("Rolling back implicit transaction block internally");
            self.backend
                .execute_statement("ROLLBACK".to_string(), vec![], vec![], vec![], vec![])
                .await?;
            self.implicit_transaction_active = false;
        }
        Ok(())
    }

    pub(crate) fn buffer_dml(&mut self, dml: BufferedDml) {
        if self.active_batch.is_none() {
            self.active_batch = Some(Batch::new_dml());
        }
        if let Some(Batch::Dml(ref mut statements)) = self.active_batch {
            statements.push(dml);
        }
    }

    pub(crate) fn maybe_buffer_dml(&mut self, portal_name: &str, max_rows: i32) -> bool {
        if let Some(portal) = self.portals.get(portal_name) {
            if crate::session::is_batchable_dml(&portal.statement_type) {
                let tag_prefix = crate::pgwire::command::parse_command_tag(&portal.sql);
                let buffered_dml = BufferedDml {
                    portal_name: portal_name.to_string(),
                    sql: portal.sql.clone(),
                    param_types: portal.param_types.clone(),
                    param_formats: portal.param_formats.clone(),
                    param_values: portal.param_values.clone(),
                    max_rows,
                    tag_prefix,
                };
                self.buffer_dml(buffered_dml);
                return true;
            }
        }
        false
    }

    pub(crate) async fn flush(
        &mut self,
    ) -> Result<Option<Vec<(String, Result<i64, anyhow::Error>)>>, anyhow::Error> {
        if let Some(batch) = self.active_batch.take() {
            if !batch.is_empty() {
                let results = batch
                    .flush(&mut self.backend, &mut self.in_error_state)
                    .await?;
                return Ok(Some(results));
            }
        }
        Ok(None)
    }
}
