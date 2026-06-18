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

use crate::backend::{BackendColumn, BackendResponse, QueryBackend};
use crate::pgwire::command::{format_command_complete_tag, parse_command_tag};
use crate::pgwire::connection::Connection;
use crate::pgwire::error::PGError;
use crate::pgwire::protocol::error::ErrorResponse;
use crate::pgwire::protocol::extended::{
    Bind, Close, DataRow, Describe, Execute, FieldDescription, ParameterDescription, Parse,
    RowDescription,
};
use crate::pgwire::protocol::handshake::ReadyForQuery;
use crate::pgwire::protocol::message::SyncMessage;
use crate::pgwire::protocol::query::{CommandComplete, Query};
use crate::pgwire::protocol::{BackendMessage, Encode, FrontendMessage};
use crate::session::{Portal, PreparedStatement};
use bytes::{Bytes, BytesMut};
use google_cloud_spanner::connection::Dialect;
use google_cloud_spanner::connection::parser::{ClientSideCommand, StatementType, parse_statement};
use tokio::io::AsyncWriteExt;
use tracing::{error, info};

pub struct ExtendedQueryExecutor;

impl ExtendedQueryExecutor {
    pub(crate) async fn handle_message<B: QueryBackend>(
        conn: &mut Connection<B>,
        message: FrontendMessage,
    ) -> Result<bool, anyhow::Error> {
        match message {
            FrontendMessage::Terminate(_) => {
                info!("client requested connection termination");
                Ok(false)
            }
            FrontendMessage::Query(query) => {
                Self::handle_simple_query(conn, query).await?;
                Ok(true)
            }
            FrontendMessage::Sync(_) => {
                conn.message_buffer.push(message);
                Self::flush_buffer(conn).await?;
                Ok(true)
            }
            FrontendMessage::Flush(_) => {
                conn.message_buffer.push(message);
                Self::flush_buffer(conn).await?;
                Ok(true)
            }
            other => {
                conn.message_buffer.push(other);
                Ok(true)
            }
        }
    }

    pub(crate) async fn flush_buffer<B: QueryBackend>(
        conn: &mut Connection<B>,
    ) -> Result<(), anyhow::Error> {
        let messages = std::mem::take(&mut conn.message_buffer);
        if messages.is_empty() {
            return Ok(());
        }

        Self::pre_analyze_and_begin_implicit_transaction(conn, &messages).await?;
        Self::process_messages(conn, messages).await
    }

    async fn pre_analyze_and_begin_implicit_transaction<B: QueryBackend>(
        conn: &mut Connection<B>,
        messages: &[FrontendMessage],
    ) -> Result<(), anyhow::Error> {
        let mut stmt_sql = std::collections::HashMap::new();
        let mut portal_stmt = std::collections::HashMap::new();
        let mut executes = Vec::new();
        let mut has_sync = false;
        for msg in messages {
            match msg {
                FrontendMessage::Parse(parse) => {
                    stmt_sql.insert(parse.name.clone(), parse.query.clone());
                }
                FrontendMessage::Bind(bind) => {
                    portal_stmt.insert(bind.portal.clone(), bind.statement.clone());
                }
                FrontendMessage::Execute(execute) => {
                    executes.push(execute);
                }
                FrontendMessage::Sync(_) => {
                    has_sync = true;
                }
                _ => {}
            }
        }

        let run_in_implicit_tx = if conn.session.backend.transaction_status() != b'I' {
            false
        } else if executes.len() > 1 {
            true
        } else if executes.len() == 1 {
            !has_sync
        } else {
            false
        };

        if run_in_implicit_tx {
            if let Err(error) = Self::determine_and_begin_implicit_transaction(
                conn,
                &executes,
                &stmt_sql,
                &portal_stmt,
            )
            .await
            {
                error!("failed to start implicit transaction: {}", error);
                Self::send_error_response(conn, &error).await?;
                conn.session.in_error_state = true;
            }
        }
        Ok(())
    }

    async fn handle_error<B: QueryBackend>(
        conn: &mut Connection<B>,
        context: &str,
        error: anyhow::Error,
    ) -> Result<(), anyhow::Error> {
        error!("error handling {}: {}", context, error);
        Self::send_error_response(conn, &error).await?;
        conn.session.in_error_state = true;
        Ok(())
    }

    async fn process_messages<B: QueryBackend>(
        conn: &mut Connection<B>,
        messages: Vec<FrontendMessage>,
    ) -> Result<(), anyhow::Error> {
        for msg in messages {
            if conn.session.in_error_state {
                match msg {
                    FrontendMessage::Sync(_) => {
                        conn.session.in_error_state = false;
                        Self::handle_sync(conn).await?;
                    }
                    FrontendMessage::Flush(_) => {
                        Self::handle_flush(conn).await?;
                    }
                    _ => {}
                }
                continue;
            }

            match msg {
                FrontendMessage::Parse(parse) => {
                    if let Err(error) = Self::handle_parse(conn, parse).await {
                        Self::handle_error(conn, "Parse", error).await?;
                    }
                }
                FrontendMessage::Bind(bind) => {
                    if let Err(error) = Self::handle_bind(conn, bind).await {
                        Self::handle_error(conn, "Bind", error).await?;
                    }
                }
                FrontendMessage::Describe(describe) => {
                    if let Err(error) = Self::handle_describe(conn, describe).await {
                        Self::handle_error(conn, "Describe", error).await?;
                    }
                }
                FrontendMessage::Execute(execute) => {
                    if conn
                        .session
                        .maybe_buffer_dml(&execute.portal, execute.max_rows)
                    {
                        info!("buffered DML execute portal: {}", execute.portal);
                    } else {
                        if let Err(error) = Self::flush_session(conn).await {
                            conn.session
                                .maybe_rollback_implicit_transaction()
                                .await
                                .ok();
                            Self::handle_error(conn, "flushing session before execute", error)
                                .await?;
                        } else if let Err(error) = Self::handle_execute(conn, execute).await {
                            conn.session
                                .maybe_rollback_implicit_transaction()
                                .await
                                .ok();
                            Self::handle_error(conn, "Execute", error).await?;
                        }
                    }
                }
                FrontendMessage::Close(close) => {
                    if let Err(error) = Self::handle_close(conn, close).await {
                        Self::handle_error(conn, "Close", error).await?;
                    }
                }
                FrontendMessage::Sync(_) => {
                    if let Err(error) = Self::flush_session(conn).await {
                        conn.session
                            .maybe_rollback_implicit_transaction()
                            .await
                            .ok();
                        Self::handle_error(conn, "flushing session on Sync", error).await?;
                    } else if conn.session.in_error_state {
                        conn.session
                            .maybe_rollback_implicit_transaction()
                            .await
                            .ok();
                    } else if let Err(error) =
                        conn.session.maybe_commit_implicit_transaction().await
                    {
                        Self::handle_error(conn, "failed to commit implicit transaction", error)
                            .await?;
                    }
                    Self::handle_sync(conn).await?;
                }
                FrontendMessage::Flush(_) => {
                    if let Err(error) = Self::flush_session(conn).await {
                        conn.session
                            .maybe_rollback_implicit_transaction()
                            .await
                            .ok();
                        Self::handle_error(conn, "flushing session on Flush", error).await?;
                    } else {
                        Self::handle_flush(conn).await?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn determine_and_begin_implicit_transaction<B: QueryBackend>(
        conn: &mut Connection<B>,
        executes: &[&Execute],
        stmt_sql: &std::collections::HashMap<String, String>,
        portal_stmt: &std::collections::HashMap<String, String>,
    ) -> Result<(), anyhow::Error> {
        let mut read_only = true;
        for exec in executes {
            let sql = if let Some(stmt_name) = portal_stmt.get(&exec.portal) {
                stmt_sql.get(stmt_name).cloned()
            } else if let Some(portal) = conn.session.portals.get(&exec.portal) {
                Some(portal.sql.clone())
            } else {
                None
            };

            if let Some(sql_str) = sql {
                let command_tag = parse_command_tag(&sql_str);
                if command_tag != "SELECT" && command_tag != "SHOW" {
                    read_only = false;
                    break;
                }
            }
        }

        let statement_type = if let Some(first_exec) = executes.first() {
            if let Some(stmt_name) = portal_stmt.get(&first_exec.portal) {
                if let Some(sql_str) = stmt_sql.get(stmt_name) {
                    parse_statement(sql_str, Dialect::PostgreSql)?
                } else {
                    StatementType::ClientSide(ClientSideCommand::Commit)
                }
            } else if let Some(portal) = conn.session.portals.get(&first_exec.portal) {
                portal.statement_type.clone()
            } else {
                StatementType::ClientSide(ClientSideCommand::Commit)
            }
        } else {
            StatementType::ClientSide(ClientSideCommand::Commit)
        };

        conn.session
            .maybe_begin_implicit_transaction(&statement_type, read_only)
            .await
    }

    async fn handle_simple_query<B: QueryBackend>(
        conn: &mut Connection<B>,
        query: Query,
    ) -> Result<(), anyhow::Error> {
        info!("executing simple query: {}", query.sql);
        conn.extended_protocol = false;
        conn.message_buffer.clear();

        let statements = Self::parse_simple_query_statements(&query.sql);
        if statements.is_empty() {
            let mut response = BytesMut::new();
            BackendMessage::EmptyQueryResponse.encode(&mut response)?;
            let tx_status = conn.session.backend.transaction_status();
            BackendMessage::ReadyForQuery(ReadyForQuery { tx_status }).encode(&mut response)?;
            conn.stream.write_all(&response).await?;
            conn.stream.flush().await?;
            conn.extended_protocol = true;
            return Ok(());
        }

        for (i, stmt) in statements.iter().enumerate() {
            let stmt_name = format!("s_{}", i);
            let portal_name = format!("p_{}", i);

            conn.message_buffer.push(FrontendMessage::Parse(Parse {
                name: stmt_name.clone(),
                query: stmt.to_string(),
                param_types: vec![],
            }));
            conn.message_buffer.push(FrontendMessage::Bind(Bind {
                portal: portal_name.clone(),
                statement: stmt_name.clone(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            }));
            conn.message_buffer
                .push(FrontendMessage::Describe(Describe {
                    desc_type: b'P',
                    name: portal_name.clone(),
                }));
            conn.message_buffer.push(FrontendMessage::Execute(Execute {
                portal: portal_name.clone(),
                max_rows: 0,
            }));
        }
        conn.message_buffer.push(FrontendMessage::Sync(SyncMessage));
        Self::flush_buffer(conn).await?;
        conn.extended_protocol = true;
        Ok(())
    }

    // TODO: This should move to the parser in the Connection API
    pub(crate) fn parse_simple_query_statements(sql: &str) -> Vec<String> {
        sql.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub(crate) async fn send_error_response<B: QueryBackend>(
        conn: &mut Connection<B>,
        error: &anyhow::Error,
    ) -> Result<(), anyhow::Error> {
        let mut response_buffer = BytesMut::new();
        write_error_response(error, &mut response_buffer)?;
        conn.stream.write_all(&response_buffer).await?;
        Ok(())
    }

    pub(crate) async fn handle_parse<B: QueryBackend>(
        conn: &mut Connection<B>,
        parse: Parse,
    ) -> Result<(), anyhow::Error> {
        info!("parsing statement '{}': {}", parse.name, parse.query);
        let statement_type = google_cloud_spanner::connection::parser::parse_statement(
            &parse.query,
            google_cloud_spanner::connection::Dialect::PostgreSql,
        )?;
        let stmt = PreparedStatement {
            sql: parse.query.clone(),
            param_types: parse.param_types.clone(),
            statement_type,
        };
        conn.session.prepared_statements.insert(parse.name, stmt);

        let mut response = BytesMut::new();
        if conn.extended_protocol {
            BackendMessage::ParseComplete.encode(&mut response)?;
            conn.stream.write_all(&response).await?;
        }
        Ok(())
    }

    pub(crate) async fn handle_bind<B: QueryBackend>(
        conn: &mut Connection<B>,
        bind: Bind,
    ) -> Result<(), anyhow::Error> {
        info!(
            "binding portal '{}' to statement '{}'",
            bind.portal, bind.statement
        );
        let stmt = conn
            .session
            .prepared_statements
            .get(&bind.statement)
            .ok_or_else(|| anyhow::anyhow!("prepared statement '{}' not found", bind.statement))?;

        let mut param_values = Vec::new();
        for val in &bind.params {
            param_values.push(val.clone());
        }

        let portal = Portal {
            sql: stmt.sql.clone(),
            param_types: stmt.param_types.clone(),
            param_formats: bind.param_formats.clone(),
            param_values,
            result_formats: bind.result_formats.clone(),
            tag: None,
            columns: None,
            row_reader: None,
            total_rows_retrieved: 0,
            statement_type: stmt.statement_type.clone(),
        };
        conn.session.portals.insert(bind.portal, portal);

        let mut response = BytesMut::new();
        if conn.extended_protocol {
            BackendMessage::BindComplete.encode(&mut response)?;
            conn.stream.write_all(&response).await?;
        }
        Ok(())
    }

    pub(crate) async fn handle_describe<B: QueryBackend>(
        conn: &mut Connection<B>,
        describe: Describe,
    ) -> Result<(), anyhow::Error> {
        Self::flush_session(conn).await?;
        let mut response = BytesMut::new();
        if describe.desc_type == b'S' {
            Self::describe_prepared_statement(conn, &describe.name, &mut response).await?;
        } else {
            Self::describe_portal(conn, &describe.name, &mut response).await?;
        }
        if !response.is_empty() {
            conn.stream.write_all(&response).await?;
        }
        Ok(())
    }

    async fn describe_prepared_statement<B: QueryBackend>(
        conn: &mut Connection<B>,
        name: &str,
        response: &mut BytesMut,
    ) -> Result<(), anyhow::Error> {
        info!("describing statement '{}'", name);
        let stmt = conn
            .session
            .prepared_statements
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("prepared statement '{}' not found", name))?;

        let desc = conn
            .session
            .backend
            .describe_query(stmt.sql.clone())
            .await?;
        if !desc.param_types.is_empty() {
            stmt.param_types = desc.param_types.clone();
        }

        if conn.extended_protocol {
            let param_types = stmt.param_types.clone();
            let param_desc = ParameterDescription { param_types };
            BackendMessage::ParameterDescription(param_desc).encode(response)?;
            write_columns_description(&desc.columns, response)?;
        }
        Ok(())
    }

    async fn describe_portal_columns<B: QueryBackend>(
        backend: &mut B,
        extended_protocol: bool,
        portal: &mut Portal,
        response: &mut BytesMut,
    ) -> Result<(), anyhow::Error> {
        let has_unspecified_params = portal.param_types.iter().any(|&t| t == 0);
        if portal.row_reader.is_none() && !has_unspecified_params {
            // Pre-emptively execute the statement to fetch the results and column descriptions.
            // This is an optimization that avoids taking a second round-trip to the Spanner backend
            // during the subsequent Execute message block.
            let response_outcome = backend
                .execute_statement(
                    portal.sql.clone(),
                    portal.param_types.clone(),
                    portal.param_formats.clone(),
                    portal.param_values.clone(),
                    portal.result_formats.clone(),
                )
                .await?;
            match response_outcome {
                BackendResponse::ResultSet { tag, columns, rows } => {
                    portal.tag = Some(tag);
                    portal.columns = Some(columns.clone());
                    portal.row_reader = Some(rows);
                    write_portal_columns_description(&columns, &portal.result_formats, response)?;
                }
                BackendResponse::CommandComplete(tag) => {
                    portal.tag = Some(tag);
                    if extended_protocol {
                        BackendMessage::NoData.encode(response)?;
                    }
                }
                _ => {}
            }
        } else if portal.row_reader.is_some() {
            // The row reader was already fetched (pre-emptively executed in a prior Describe).
            // We just write the cached column descriptions.
            let columns = portal.columns.as_ref().unwrap();
            write_portal_columns_description(columns, &portal.result_formats, response)?;
        } else {
            // The query contains unspecified parameters (OID 0), so we cannot execute it pre-emptively.
            // We describe the query schema on Spanner in PLAN mode instead to get the column metadata.
            let desc = backend.describe_query(portal.sql.clone()).await?;
            portal.columns = Some(desc.columns.clone());
            write_portal_columns_description(&desc.columns, &portal.result_formats, response)?;
        }
        Ok(())
    }

    async fn describe_portal<B: QueryBackend>(
        conn: &mut Connection<B>,
        name: &str,
        response: &mut BytesMut,
    ) -> Result<(), anyhow::Error> {
        info!("describing portal '{}'", name);
        let extended_protocol = conn.extended_protocol;
        let portal = conn
            .session
            .portals
            .get_mut(name)
            .ok_or_else(|| anyhow::anyhow!("portal '{}' not found", name))?;

        let has_columns = match portal.statement_type {
            StatementType::Query => true,
            StatementType::ClientSide(ref cmd) => matches!(cmd, ClientSideCommand::Show { .. }),
            _ => false,
        };

        if has_columns {
            Self::describe_portal_columns(
                &mut conn.session.backend,
                extended_protocol,
                portal,
                response,
            )
            .await?;
        } else if conn.extended_protocol {
            BackendMessage::NoData.encode(response)?;
        }
        Ok(())
    }

    pub(crate) async fn handle_execute<B: QueryBackend>(
        conn: &mut Connection<B>,
        execute: Execute,
    ) -> Result<(), anyhow::Error> {
        info!(
            "executing portal '{}' (max_rows: {})",
            execute.portal, execute.max_rows
        );
        let statement_type = conn
            .session
            .portals
            .get(&execute.portal)
            .ok_or_else(|| anyhow::anyhow!("portal '{}' not found", execute.portal))?
            .statement_type
            .clone();

        let (skip_backend, custom_response) =
            Self::check_and_intercept_client_command(conn, &statement_type);

        let portal = conn.session.portals.get_mut(&execute.portal).unwrap();

        let mut response = BytesMut::new();

        if portal.row_reader.is_none() && portal.tag.is_none() {
            let response_outcome = if skip_backend {
                custom_response.unwrap()
            } else {
                conn.session
                    .backend
                    .execute_statement(
                        portal.sql.clone(),
                        portal.param_types.clone(),
                        portal.param_formats.clone(),
                        portal.param_values.clone(),
                        portal.result_formats.clone(),
                    )
                    .await?
            };

            match response_outcome {
                BackendResponse::CommandComplete(tag) => {
                    Self::write_command_complete(&tag, &mut response)?;
                }
                BackendResponse::UpdateComplete { tag_prefix, count } => {
                    let tag = format_command_complete_tag(&tag_prefix, count as usize);
                    Self::write_command_complete(&tag, &mut response)?;
                }
                BackendResponse::ResultSet { tag, columns, rows } => {
                    portal.tag = Some(tag);
                    portal.columns = Some(columns);
                    portal.row_reader = Some(rows);
                }
            }
        }

        if let Some(row_reader) = portal.row_reader.take() {
            let tag = portal.tag.as_ref().unwrap().clone();
            let next_reader =
                write_result_set(portal, execute.max_rows, &tag, row_reader, &mut response).await?;
            portal.row_reader = next_reader;
        }
        conn.stream.write_all(&response).await?;
        Ok(())
    }

    pub(crate) async fn handle_close<B: QueryBackend>(
        conn: &mut Connection<B>,
        close: Close,
    ) -> Result<(), anyhow::Error> {
        if close.close_type == b'S' {
            info!("closing statement '{}'", close.name);
            conn.session.prepared_statements.remove(&close.name);
        } else {
            info!("closing portal '{}'", close.name);
            conn.session.portals.remove(&close.name);
        }

        let mut response = BytesMut::new();
        BackendMessage::CloseComplete.encode(&mut response)?;
        conn.stream.write_all(&response).await?;
        Ok(())
    }

    pub(crate) async fn handle_sync<B: QueryBackend>(
        conn: &mut Connection<B>,
    ) -> Result<(), anyhow::Error> {
        info!("syncing session");
        let tx_status = conn.session.backend.transaction_status();
        let ready_for_query = ReadyForQuery { tx_status };
        let mut response = BytesMut::new();
        BackendMessage::ReadyForQuery(ready_for_query).encode(&mut response)?;
        conn.stream.write_all(&response).await?;
        conn.stream.flush().await?;
        Ok(())
    }

    pub(crate) async fn handle_flush<B: QueryBackend>(
        conn: &mut Connection<B>,
    ) -> Result<(), anyhow::Error> {
        info!("flushing stream");
        conn.stream.flush().await?;
        Ok(())
    }

    fn write_command_complete(tag: &str, response: &mut BytesMut) -> Result<(), anyhow::Error> {
        let cc = CommandComplete { tag };
        BackendMessage::CommandComplete(cc).encode(response)?;
        Ok(())
    }

    fn check_and_intercept_client_command<B: QueryBackend>(
        conn: &mut Connection<B>,
        statement_type: &StatementType,
    ) -> (bool, Option<BackendResponse>) {
        if let StatementType::ClientSide(cmd) = statement_type {
            match cmd {
                ClientSideCommand::Begin { .. } => {
                    if conn.session.implicit_transaction_active {
                        info!("Transitioning implicit transaction to explicit");
                        conn.session.implicit_transaction_active = false;
                        return (
                            true,
                            Some(BackendResponse::CommandComplete("BEGIN".to_string())),
                        );
                    } else if conn.session.backend.transaction_status() != b'I' {
                        info!("Ignoring BEGIN since transaction is already active");
                        return (
                            true,
                            Some(BackendResponse::CommandComplete("BEGIN".to_string())),
                        );
                    } else {
                        conn.session.implicit_transaction_active = false;
                    }
                }
                ClientSideCommand::Commit | ClientSideCommand::Rollback => {
                    if conn.session.backend.transaction_status() == b'I' {
                        let tag = match cmd {
                            ClientSideCommand::Commit => "COMMIT",
                            _ => "ROLLBACK",
                        };
                        info!("Ignoring {} since no transaction is active", tag);
                        return (
                            true,
                            Some(BackendResponse::CommandComplete(tag.to_string())),
                        );
                    } else {
                        conn.session.implicit_transaction_active = false;
                    }
                }
                _ => {}
            }
        }
        (false, None)
    }

    async fn flush_session<B: QueryBackend>(conn: &mut Connection<B>) -> Result<(), anyhow::Error> {
        match conn.session.flush().await {
            Ok(None) => Ok(()),
            Ok(Some(results)) => {
                let mut response = BytesMut::new();
                for (tag_prefix, res) in results {
                    match res {
                        Ok(update_count) => {
                            let tag =
                                format_command_complete_tag(&tag_prefix, update_count as usize);
                            Self::write_command_complete(&tag, &mut response)?;
                        }
                        Err(error) => {
                            write_error_response(&error, &mut response)?;
                            conn.session.in_error_state = true;
                            break;
                        }
                    }
                }
                conn.stream.write_all(&response).await?;
                Ok(())
            }
            Err(error) => {
                let mut response = BytesMut::new();
                write_error_response(&error, &mut response)?;
                conn.stream.write_all(&response).await?;
                Ok(())
            }
        }
    }
}

pub(crate) fn write_columns_description(
    columns: &[BackendColumn],
    response: &mut BytesMut,
) -> Result<(), anyhow::Error> {
    if columns.is_empty() {
        BackendMessage::NoData.encode(response)?;
    } else {
        let fields: Vec<FieldDescription> = columns
            .iter()
            .map(|col| FieldDescription {
                name: &col.name,
                table_oid: 0,
                column_index: 0,
                type_oid: col.type_oid,
                type_size: col.type_size,
                type_modifier: -1,
                format_code: col.format_code,
            })
            .collect();
        let row_desc = RowDescription { fields };
        BackendMessage::RowDescription(row_desc).encode(response)?;
    }
    Ok(())
}

pub(crate) fn write_portal_columns_description(
    columns: &[BackendColumn],
    result_formats: &[i16],
    response: &mut BytesMut,
) -> Result<(), anyhow::Error> {
    if columns.is_empty() {
        BackendMessage::NoData.encode(response)?;
    } else {
        let mut fields = Vec::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            let format_code = if result_formats.is_empty() {
                0
            } else if result_formats.len() == 1 {
                result_formats[0]
            } else {
                *result_formats.get(i).unwrap_or(&0)
            };
            fields.push(FieldDescription {
                name: &col.name,
                table_oid: 0,
                column_index: 0,
                type_oid: col.type_oid,
                type_size: col.type_size,
                type_modifier: -1,
                format_code,
            });
        }
        let row_desc = RowDescription { fields };
        BackendMessage::RowDescription(row_desc).encode(response)?;
    }
    Ok(())
}

pub(crate) async fn write_result_set(
    portal: &mut Portal,
    max_rows: i32,
    tag: &str,
    mut row_reader: Box<dyn crate::backend::RowReader>,
    response: &mut BytesMut,
) -> Result<Option<Box<dyn crate::backend::RowReader>>, anyhow::Error> {
    let limit = if max_rows > 0 {
        max_rows as usize
    } else {
        usize::MAX
    };
    let mut count = 0;
    let mut finished = false;

    while count < limit {
        if let Some(row) = row_reader.next_row().await? {
            write_result_rows(&[row], response)?;
            count += 1;
        } else {
            finished = true;
            break;
        }
    }

    portal.total_rows_retrieved += count;

    if !finished && max_rows > 0 {
        BackendMessage::PortalSuspended.encode(response)?;
        Ok(Some(row_reader))
    } else {
        let final_tag = format_command_complete_tag(tag, portal.total_rows_retrieved);
        let cc = CommandComplete { tag: &final_tag };
        BackendMessage::CommandComplete(cc).encode(response)?;
        Ok(None)
    }
}

pub(crate) fn write_result_rows(
    rows: &[Vec<Option<Bytes>>],
    response: &mut BytesMut,
) -> Result<(), anyhow::Error> {
    for row in rows {
        let values_refs: Vec<Option<&[u8]>> =
            row.iter().map(|v| v.as_ref().map(|b| &b[..])).collect();
        let dr = DataRow {
            values: values_refs,
        };
        BackendMessage::DataRow(dr).encode(response)?;
    }
    Ok(())
}

pub(crate) fn write_error_response(
    error: &anyhow::Error,
    response: &mut BytesMut,
) -> Result<(), anyhow::Error> {
    let pg_error = PGError::from(error);
    let mut error_response =
        ErrorResponse::new(&pg_error.severity, &pg_error.code, &pg_error.message);
    if let Some(ref hint) = pg_error.hint {
        error_response = error_response.with_hint(hint);
    }
    if let Some(ref detail) = pg_error.detail {
        error_response = error_response.with_detail(detail);
    }
    BackendMessage::ErrorResponse(error_response).encode(response)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use google_cloud_spanner::connection::parser::StatementType;

    struct TestRowReader {
        rows: std::vec::IntoIter<Vec<Option<Bytes>>>,
    }

    #[async_trait::async_trait]
    impl crate::backend::RowReader for TestRowReader {
        async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, anyhow::Error> {
            Ok(self.rows.next())
        }
    }

    #[tokio::test]
    async fn test_write_result_set_all_rows() {
        let mut portal = Portal {
            sql: "SELECT 1".to_string(),
            param_types: vec![],
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![0],
            tag: None,
            columns: None,
            row_reader: None,
            total_rows_retrieved: 0,
            statement_type: StatementType::Query,
        };

        let rows = vec![vec![Some(Bytes::from("1"))], vec![Some(Bytes::from("2"))]];
        let reader = Box::new(TestRowReader {
            rows: rows.into_iter(),
        });
        let mut response = BytesMut::new();

        let result = write_result_set(&mut portal, 0, "SELECT", reader, &mut response).await;
        assert!(result.is_ok());
        let remaining_reader = result.unwrap();
        assert!(remaining_reader.is_none());
        assert_eq!(portal.total_rows_retrieved, 2);
    }

    #[tokio::test]
    async fn test_write_result_set_limit_rows() {
        let mut portal = Portal {
            sql: "SELECT 1".to_string(),
            param_types: vec![],
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![0],
            tag: None,
            columns: None,
            row_reader: None,
            total_rows_retrieved: 0,
            statement_type: StatementType::Query,
        };

        let rows = vec![
            vec![Some(Bytes::from("1"))],
            vec![Some(Bytes::from("2"))],
            vec![Some(Bytes::from("3"))],
        ];
        let reader = Box::new(TestRowReader {
            rows: rows.into_iter(),
        });
        let mut response = BytesMut::new();

        let result = write_result_set(&mut portal, 2, "SELECT", reader, &mut response).await;
        assert!(result.is_ok());
        let remaining_reader = result.unwrap();
        assert!(remaining_reader.is_some());
        assert_eq!(portal.total_rows_retrieved, 2);

        let reader2 = remaining_reader.unwrap();
        let mut response2 = BytesMut::new();
        let result2 = write_result_set(&mut portal, 0, "SELECT", reader2, &mut response2).await;
        assert!(result2.is_ok());
        let remaining_reader2 = result2.unwrap();
        assert!(remaining_reader2.is_none());
        assert_eq!(portal.total_rows_retrieved, 3);
    }
}
