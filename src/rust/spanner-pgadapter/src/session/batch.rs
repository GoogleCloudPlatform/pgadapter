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

use crate::backend::{BackendResponse, QueryBackend};
use bytes::Bytes;
use google_cloud_spanner::connection::parser::StatementType;

pub(crate) fn is_batchable_dml(statement_type: &StatementType) -> bool {
    matches!(
        statement_type,
        StatementType::Update {
            has_returning: false
        }
    )
}

pub struct BufferedDml {
    pub portal_name: String,
    pub sql: String,
    pub param_types: Vec<u32>,
    pub param_formats: Vec<i16>,
    pub param_values: Vec<Option<Bytes>>,
    pub max_rows: i32,
    pub tag_prefix: String,
}

pub enum Batch {
    Dml(Vec<BufferedDml>),
}

impl Batch {
    pub fn new_dml() -> Self {
        Batch::Dml(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Batch::Dml(statements) => statements.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Batch::Dml(statements) => statements.len(),
        }
    }

    pub(crate) async fn flush<B: QueryBackend>(
        self,
        backend: &mut B,
        in_error_state: &mut bool,
    ) -> Result<Vec<(String, Result<i64, anyhow::Error>)>, anyhow::Error> {
        match self {
            Batch::Dml(dmls_to_exec) => {
                if dmls_to_exec.is_empty() {
                    return Ok(Vec::new());
                }

                if dmls_to_exec.len() == 1 {
                    Self::flush_single_dml(
                        dmls_to_exec.into_iter().next().unwrap(),
                        backend,
                        in_error_state,
                    )
                    .await
                } else {
                    Self::flush_multiple_dmls(dmls_to_exec, backend, in_error_state).await
                }
            }
        }
    }

    async fn flush_single_dml<B: QueryBackend>(
        dml: BufferedDml,
        backend: &mut B,
        in_error_state: &mut bool,
    ) -> Result<Vec<(String, Result<i64, anyhow::Error>)>, anyhow::Error> {
        let response_outcome = backend
            .execute_statement(
                dml.sql.clone(),
                dml.param_types.clone(),
                dml.param_formats.clone(),
                dml.param_values.clone(),
                vec![0], // default text format
            )
            .await;

        match response_outcome {
            Ok(BackendResponse::UpdateComplete { count, .. }) => {
                Ok(vec![(dml.tag_prefix.clone(), Ok(count))])
            }
            Ok(BackendResponse::CommandComplete(tag)) => Err(anyhow::anyhow!(
                "unexpected non-DML CommandComplete for buffered DML: {}",
                tag
            )),
            Ok(BackendResponse::ResultSet { .. }) => {
                Err(anyhow::anyhow!("buffered DML query returned a result set"))
            }
            Err(error) => {
                *in_error_state = true;
                Ok(vec![(dml.tag_prefix.clone(), Err(error))])
            }
        }
    }

    async fn flush_multiple_dmls<B: QueryBackend>(
        dmls_to_exec: Vec<BufferedDml>,
        backend: &mut B,
        in_error_state: &mut bool,
    ) -> Result<Vec<(String, Result<i64, anyhow::Error>)>, anyhow::Error> {
        let mut batch = Vec::new();
        for dml in &dmls_to_exec {
            batch.push((
                dml.sql.clone(),
                dml.param_types.clone(),
                dml.param_formats.clone(),
                dml.param_values.clone(),
            ));
        }

        let results = backend.execute_batch_dml(batch).await;
        match results {
            Ok(batch_results) => {
                let mut mapped = Vec::new();
                for (i, res) in batch_results.into_iter().enumerate() {
                    let dml = &dmls_to_exec[i];
                    match res {
                        Ok(count) => mapped.push((dml.tag_prefix.clone(), Ok(count))),
                        Err(error) => {
                            *in_error_state = true;
                            mapped.push((dml.tag_prefix.clone(), Err(error)));
                            break;
                        }
                    }
                }
                Ok(mapped)
            }
            Err(error) => {
                *in_error_state = true;
                Err(error)
            }
        }
    }
}
