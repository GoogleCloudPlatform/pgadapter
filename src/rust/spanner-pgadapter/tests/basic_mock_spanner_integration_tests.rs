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

mod common;
mod mock_spanner;

use serde_json;
use tokio_postgres::NoTls;
use tracing::{debug, error};

use mock_spanner::google::spanner::v1::Session;
use mock_spanner::google::spanner::v1::{
    ResultSetMetadata, ResultSetStats, StructType, Transaction, Type, TypeCode,
    execute_sql_request::QueryMode, result_set_stats::RowCount, struct_type::Field,
    transaction_selector::Selector as TxSelector,
};
use mock_spanner::helpers::{make_int64_partial_result_set, make_string_partial_result_set};

#[tokio::test]
async fn test_mock_spanner_select_1() {
    common::init_test();

    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner: Received dialect detection query: {}", sql);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock execute_streaming_sql (SELECT 1 query - Describe/Plan phase)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            req.get_ref().sql.contains("SELECT 1")
                && req.get_ref().query_mode == QueryMode::Plan as i32
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_req| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let mut metadata = ResultSetMetadata::default();
            let mut fields = Vec::new();
            let mut int_type = Type::default();
            int_type.code = TypeCode::Int64 as i32;
            fields.push(Field {
                name: "?column?".to_string(),
                r#type: Some(int_type),
            });
            metadata.row_type = Some(StructType { fields });
            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(metadata);

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 5. Mock execute_streaming_sql (SELECT 1 query - Execute/Normal phase)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::SingleUse(opts) => {
                        opts.mode.as_ref().map(|m| match m {
                            mock_spanner::google::spanner::v1::transaction_options::Mode::ReadOnly(_) => true,
                            _ => false,
                        }).unwrap_or(false)
                    }
                    _ => false,
                })
                .unwrap_or(false);
            request.sql.contains("SELECT 1")
                && request.query_mode == QueryMode::Normal as i32
                && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_req| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs = make_int64_partial_result_set(vec!["?column?"], vec![vec![1]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // Start mock Spanner server
    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");
    debug!("Mock Spanner server listening on: {}", spanner_uri);

    // Start PGAdapter test server using SpannerBackend
    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    // Execute query
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("failed to execute SELECT 1");

    assert_eq!(rows.len(), 1);
    let val: i64 = rows[0].get(0);
    assert_eq!(val, 1);
}

#[tokio::test]
async fn test_mock_spanner_select_all_types() {
    common::init_test();

    let mut mock_service = mock_spanner::MockSpanner::new();

    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (AllTypes): Received dialect detection query: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock execute_streaming_sql (SELECT * FROM all_types - Describe/Plan phase)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            req.get_ref().sql.contains("SELECT * FROM all_types")
                && req.get_ref().query_mode == QueryMode::Plan as i32
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (AllTypes Plan): Received query: {}", sql);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs = mock_spanner::helpers::make_all_types_partial_result_set();
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 4. Mock execute_streaming_sql (SELECT * FROM all_types - Execute/Normal phase)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::SingleUse(opts) => {
                        opts.mode.as_ref().map(|m| match m {
                            mock_spanner::google::spanner::v1::transaction_options::Mode::ReadOnly(_) => true,
                            _ => false,
                        }).unwrap_or(false)
                    }
                    _ => false,
                })
                .unwrap_or(false);
            request.sql.contains("SELECT * FROM all_types")
                && request.query_mode == QueryMode::Normal as i32
                && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (AllTypes Exec): Received query: {}", sql);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs = mock_spanner::helpers::make_all_types_partial_result_set();
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    let rows = client
        .query("SELECT * FROM all_types", &[])
        .await
        .expect("failed to execute SELECT * FROM all_types");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let col_bool: bool = row.get("col_bool");
    assert_eq!(col_bool, true);

    let col_int8: i64 = row.get("col_int8");
    assert_eq!(col_int8, 123456789i64);

    let col_int4: i64 = row.get("col_int4");
    assert_eq!(col_int4, 12345i64);

    let col_float4: f32 = row.get("col_float4");
    assert_eq!(col_float4, 12.34f32);

    let col_float8: f64 = row.get("col_float8");
    assert_eq!(col_float8, 56.78f64);

    let col_text: String = row.get("col_text");
    assert_eq!(col_text, "hello text");

    let col_bytea: Vec<u8> = row.get("col_bytea");
    assert_eq!(col_bytea, b"hello bytes".to_vec());

    let col_jsonb: serde_json::Value = row.get("col_jsonb");
    assert_eq!(col_jsonb, serde_json::json!({"key": "value"}));

    let col_arr_bool: Vec<bool> = row.get("col_arr_bool");
    assert_eq!(col_arr_bool, vec![true, false]);

    let col_arr_int8: Vec<i64> = row.get("col_arr_int8");
    assert_eq!(col_arr_int8, vec![100, 200]);

    let col_arr_int4: Vec<i64> = row.get("col_arr_int4");
    assert_eq!(col_arr_int4, vec![10, 20]);

    let col_arr_text: Vec<String> = row.get("col_arr_text");
    assert_eq!(col_arr_text, vec!["a".to_string(), "b".to_string()]);

    let col_arr_float4: Vec<f32> = row.get("col_arr_float4");
    assert_eq!(col_arr_float4, vec![1.1f32, 2.2f32]);

    let col_arr_float8: Vec<f64> = row.get("col_arr_float8");
    assert_eq!(col_arr_float8, vec![3.3f64, 4.4f64]);
}

#[tokio::test]
async fn test_mock_spanner_insert_all_types() {
    common::init_test();

    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (Insert): Received dialect detection query: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock execute_streaming_sql (INSERT Describe/Plan phase - runs inside Describe transaction runner)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            req.get_ref().sql.contains("INSERT INTO all_types")
                && req.get_ref().query_mode == QueryMode::Plan as i32
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (Insert Plan): Received query: {}", sql);
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let make_type = |code: TypeCode| {
                let mut t = Type::default();
                t.code = code as i32;
                t
            };

            let make_array_type = |elem_code: TypeCode| {
                let mut elem_type = Type::default();
                elem_type.code = elem_code as i32;
                let mut arr_type = Type::default();
                arr_type.code = TypeCode::Array as i32;
                arr_type.array_element_type = Some(Box::new(elem_type));
                arr_type
            };

            let mut fields = Vec::new();
            let mut add_param = |name: &str, t: Type| {
                fields.push(Field {
                    name: name.to_string(),
                    r#type: Some(t),
                });
            };

            add_param("p1", make_type(TypeCode::Bool));
            add_param("p2", make_type(TypeCode::Int64));
            add_param("p3", make_type(TypeCode::Int64));
            add_param("p4", make_type(TypeCode::Float32));
            add_param("p5", make_type(TypeCode::Float64));
            add_param("p6", make_type(TypeCode::String));
            add_param("p7", make_type(TypeCode::Bytes));
            add_param("p8", make_type(TypeCode::Json));
            add_param("p9", make_array_type(TypeCode::Bool));
            add_param("p10", make_array_type(TypeCode::Int64));
            add_param("p11", make_array_type(TypeCode::Int64));
            add_param("p12", make_array_type(TypeCode::String));
            add_param("p13", make_array_type(TypeCode::Float32));
            add_param("p14", make_array_type(TypeCode::Float64));

            let mut metadata = ResultSetMetadata::default();
            metadata.undeclared_parameters = Some(StructType { fields });
            if let Some(ref selector) = request.transaction {
                if let Some(TxSelector::Begin(_)) = selector.selector {
                    metadata.transaction = Some(Transaction {
                        id: b"tx_describe".to_vec(),
                        read_timestamp: None,
                        precommit_token: None,
                        cache_update: None,
                    });
                }
            }

            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(metadata);

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 4. Mock commit for Describe transaction runner
    mock_service
        .expect_commit()
        .withf(|req| match &req.get_ref().transaction {
            Some(
                mock_spanner::google::spanner::v1::commit_request::Transaction::TransactionId(id),
            ) => id == b"tx_describe",
            _ => false,
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::CommitResponse::default(),
            ))
        });

    // 5. Mock execute_sql (INSERT Execute/Normal phase)
    mock_service
        .expect_execute_sql()
        .withf(|req| {
            let request = req.get_ref();
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::Begin(_) => true,
                    _ => false,
                })
                .unwrap_or(false);
            request.sql.contains("INSERT INTO all_types")
                && request.query_mode == QueryMode::Normal as i32
                && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (Insert Exec): Received execute_sql: {}", sql);

            let params = request.params.unwrap();

            assert_eq!(
                params.fields.get("p1").unwrap().kind,
                Some(prost_types::value::Kind::BoolValue(true))
            );
            assert_eq!(
                params.fields.get("p2").unwrap().kind,
                Some(prost_types::value::Kind::StringValue(
                    "123456789".to_string()
                ))
            );
            assert_eq!(
                params.fields.get("p3").unwrap().kind,
                Some(prost_types::value::Kind::StringValue("12345".to_string()))
            );

            if let Some(prost_types::value::Kind::NumberValue(num)) =
                params.fields.get("p4").unwrap().kind
            {
                assert!((num - 12.34f64).abs() < 1e-5);
            } else {
                panic!("expected NumberValue for col_float4");
            }

            if let Some(prost_types::value::Kind::NumberValue(num)) =
                params.fields.get("p5").unwrap().kind
            {
                assert!((num - 56.78f64).abs() < 1e-5);
            } else {
                panic!("expected NumberValue for col_float8");
            }

            assert_eq!(
                params.fields.get("p6").unwrap().kind,
                Some(prost_types::value::Kind::StringValue(
                    "hello text".to_string()
                ))
            );

            use base64::Engine;
            let expected_base64 = base64::engine::general_purpose::STANDARD.encode(b"hello bytes");
            assert_eq!(
                params.fields.get("p7").unwrap().kind,
                Some(prost_types::value::Kind::StringValue(expected_base64))
            );

            if let Some(prost_types::value::Kind::StringValue(ref s)) =
                params.fields.get("p8").unwrap().kind
            {
                let parsed: serde_json::Value = serde_json::from_str(s).unwrap();
                assert_eq!(parsed, serde_json::json!({"key": "value"}));
            } else {
                panic!("expected StringValue for col_jsonb");
            }

            if let Some(prost_types::value::Kind::ListValue(ref list)) =
                params.fields.get("p9").unwrap().kind
            {
                assert_eq!(list.values.len(), 2);
                assert_eq!(
                    list.values[0].kind,
                    Some(prost_types::value::Kind::BoolValue(true))
                );
                assert_eq!(
                    list.values[1].kind,
                    Some(prost_types::value::Kind::BoolValue(false))
                );
            } else {
                panic!("expected ListValue for col_arr_bool");
            }

            let mut metadata = ResultSetMetadata::default();
            metadata.transaction = Some(Transaction {
                id: b"tx_execute".to_vec(),
                read_timestamp: None,
                precommit_token: None,
                cache_update: None,
            });

            let mut stats = ResultSetStats::default();
            stats.row_count = Some(RowCount::RowCountExact(1));

            let mut rs = mock_spanner::google::spanner::v1::ResultSet::default();
            rs.metadata = Some(metadata);
            rs.stats = Some(stats);
            Ok(tonic::Response::new(rs))
        });

    // 6. Mock commit for Execute transaction runner
    mock_service
        .expect_commit()
        .withf(|req| match &req.get_ref().transaction {
            Some(
                mock_spanner::google::spanner::v1::commit_request::Transaction::TransactionId(id),
            ) => id == b"tx_execute",
            _ => false,
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::CommitResponse::default(),
            ))
        });

    // 7. Mock rollback (never expected on successful commit)
    mock_service
        .expect_rollback()
        .times(0)
        .returning(|_| Ok(tonic::Response::new(())));

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    let affected = client
        .execute(
            "INSERT INTO all_types (col_bool, col_int8, col_int4, col_float4, col_float8, col_text, col_bytea, col_jsonb, col_arr_bool, col_arr_int8, col_arr_int4, col_arr_text, col_arr_float4, col_arr_float8) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            &[
                &true,
                &123456789i64,
                &12345i64,
                &12.34f32,
                &56.78f64,
                &"hello text",
                &b"hello bytes".to_vec(),
                &serde_json::json!({"key": "value"}),
                &vec![true, false],
                &vec![100i64, 200i64],
                &vec![10i64, 20i64],
                &vec!["a".to_string(), "b".to_string()],
                &vec![1.1f32, 2.2f32],
                &vec![3.3f64, 4.4f64],
            ],
        )
        .await
        .expect("failed to execute insert statement");

    assert_eq!(affected, 1);
}

#[tokio::test]
async fn test_mock_spanner_read_write_transaction() {
    common::init_test();
    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (RW Tx): Received dialect detection query: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock begin_transaction
    mock_service
        .expect_begin_transaction()
        .withf(|req| {
            let options = req
                .get_ref()
                .options
                .as_ref()
                .expect("expected transaction options");
            matches!(
                options.mode,
                Some(mock_spanner::google::spanner::v1::transaction_options::Mode::ReadWrite(_))
            )
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: b"tx_rw_1".to_vec(),
                read_timestamp: None,
                precommit_token: None,
                cache_update: None,
            }))
        });

    // 4. Mock execute_streaming_sql (INSERT Describe/Plan phase)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            req.get_ref().sql.contains("INSERT INTO all_types")
                && req.get_ref().query_mode == QueryMode::Plan as i32
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (RW Tx Plan): Received query: {}", sql);
            assert_eq!(
                request.transaction.unwrap().selector.unwrap(),
                TxSelector::Id(b"tx_rw_1".to_vec())
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let mut fields = Vec::new();
            let make_type = |code: TypeCode| {
                let mut t = Type::default();
                t.code = code as i32;
                t
            };
            fields.push(Field {
                name: "p1".to_string(),
                r#type: Some(make_type(TypeCode::Bool)),
            });

            let mut metadata = ResultSetMetadata::default();
            metadata.undeclared_parameters = Some(StructType { fields });
            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(metadata);

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 5. Mock execute_sql (INSERT Execute/Normal phase)
    mock_service
        .expect_execute_sql()
        .withf(|req| {
            req.get_ref().sql.contains("INSERT INTO all_types")
                && req.get_ref().query_mode == QueryMode::Normal as i32
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (RW Tx Exec): Received execute_sql: {}", sql);
            assert_eq!(
                request.transaction.unwrap().selector.unwrap(),
                TxSelector::Id(b"tx_rw_1".to_vec())
            );
            let mut stats = ResultSetStats::default();
            stats.row_count = Some(RowCount::RowCountExact(1));

            let mut rs = mock_spanner::google::spanner::v1::ResultSet::default();
            rs.stats = Some(stats);
            Ok(tonic::Response::new(rs))
        });

    // 6. Mock commit
    mock_service
        .expect_commit()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            match request.transaction {
                Some(
                    mock_spanner::google::spanner::v1::commit_request::Transaction::TransactionId(
                        id,
                    ),
                ) => {
                    assert_eq!(id, b"tx_rw_1".to_vec());
                }
                _ => panic!("expected TransactionId"),
            }
            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::CommitResponse::default(),
            ))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (mut client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    let tx = client
        .transaction()
        .await
        .expect("failed to start Postgres transaction");
    let affected = tx
        .execute("INSERT INTO all_types (col_bool) VALUES ($1)", &[&true])
        .await
        .expect("insert failed");
    assert_eq!(affected, 1);

    tx.commit().await.expect("commit failed");
}

#[tokio::test]
async fn test_mock_spanner_read_only_transaction() {
    common::init_test();
    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (RO Tx): Received dialect detection query: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock execute_streaming_sql (SELECT Describe/Plan phase - begins read-only transaction inline)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let has_query = request.sql.contains("SELECT col_bool FROM all_types")
                && request.query_mode == QueryMode::Plan as i32;
            let options = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .and_then(|s| match s {
                    TxSelector::Begin(opts) => Some(opts),
                    _ => None,
                });
            let is_readonly = options
                .and_then(|o| o.mode.as_ref())
                .map(|m| {
                    matches!(
                        m,
                        mock_spanner::google::spanner::v1::transaction_options::Mode::ReadOnly(_)
                    )
                })
                .unwrap_or(false);
            has_query && is_readonly
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (RO Tx Plan): Received query: {}", sql);
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let make_type = |code: TypeCode| {
                let mut t = Type::default();
                t.code = code as i32;
                t
            };
            let mut fields = Vec::new();
            fields.push(Field {
                name: "p1".to_string(),
                r#type: Some(make_type(TypeCode::Int64)),
            });

            let mut metadata = ResultSetMetadata::default();
            metadata.undeclared_parameters = Some(StructType { fields });

            // Populate row_type for SELECT columns
            let mut row_fields = Vec::new();
            row_fields.push(Field {
                name: "col_bool".to_string(),
                r#type: Some(make_type(TypeCode::Bool)),
            });
            metadata.row_type = Some(StructType { fields: row_fields });

            // Return transaction ID and read timestamp inline
            metadata.transaction = Some(Transaction {
                id: b"tx_ro_1".to_vec(),
                read_timestamp: Some(prost_types::Timestamp {
                    seconds: 1672531199,
                    nanos: 0,
                }),
                precommit_token: None,
                cache_update: None,
            });

            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(metadata);

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 4. Mock execute_streaming_sql (SELECT Execute/Normal phase - uses transaction ID tx_ro_1)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let has_query = request.sql.contains("SELECT col_bool FROM all_types")
                && request.query_mode == QueryMode::Normal as i32;
            let uses_tx_ro_1 = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::Id(id) => id == b"tx_ro_1",
                    _ => false,
                })
                .unwrap_or(false);
            has_query && uses_tx_ro_1
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (RO Tx Exec): Received execute_streaming_sql: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let mut fields = Vec::new();
            let make_type = |code: TypeCode| {
                let mut t = Type::default();
                t.code = code as i32;
                t
            };
            fields.push(Field {
                name: "col_bool".to_string(),
                r#type: Some(make_type(TypeCode::Bool)),
            });

            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(ResultSetMetadata {
                row_type: Some(StructType { fields }),
                transaction: None,
                undeclared_parameters: None,
            });
            prs.values.push(prost_types::Value {
                kind: Some(prost_types::value::Kind::BoolValue(true)),
            });

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (mut client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    let tx = client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .expect("failed to start Postgres RO transaction");
    let rows = tx
        .query(
            "SELECT col_bool FROM all_types WHERE col_int8 = $1",
            &[&123456789i64],
        )
        .await
        .expect("query failed");
    assert_eq!(rows.len(), 1);
    let val: bool = rows[0].get(0);
    assert_eq!(val, true);

    tx.commit().await.expect("commit RO transaction failed");
}

#[tokio::test]
async fn test_mock_spanner_batch_dml() {
    common::init_test();
    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!(
                "MockSpanner (Batch): Received dialect detection query: {}",
                sql
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock begin_transaction
    mock_service
        .expect_begin_transaction()
        .withf(|req| {
            let options = req
                .get_ref()
                .options
                .as_ref()
                .expect("expected transaction options");
            matches!(
                options.mode,
                Some(mock_spanner::google::spanner::v1::transaction_options::Mode::ReadWrite(_))
            )
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            Ok(tonic::Response::new(Transaction {
                id: b"tx_rw_2".to_vec(),
                read_timestamp: None,
                precommit_token: None,
                cache_update: None,
            }))
        });

    // 4. Mock execute_streaming_sql (INSERT Describe/Plan phase - 2 parallel calls)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            req.get_ref().sql.contains("INSERT INTO all_types")
                && req.get_ref().query_mode == QueryMode::Plan as i32
        })
        .times(2)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (Batch Plan): Received query: {}", sql);
            assert_eq!(
                request.transaction.unwrap().selector.unwrap(),
                TxSelector::Id(b"tx_rw_2".to_vec())
            );
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            let mut fields = Vec::new();
            let make_type = |code: TypeCode| {
                let mut t = Type::default();
                t.code = code as i32;
                t
            };
            fields.push(Field {
                name: "p1".to_string(),
                r#type: Some(make_type(TypeCode::Bool)),
            });

            let mut metadata = ResultSetMetadata::default();
            metadata.undeclared_parameters = Some(StructType { fields });
            let mut prs = mock_spanner::google::spanner::v1::PartialResultSet::default();
            prs.metadata = Some(metadata);

            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 5. Mock execute_sql (INSERT Execute/Normal phase - 2 parallel calls)
    mock_service
        .expect_execute_sql()
        .withf(|req| {
            req.get_ref().sql.contains("INSERT INTO all_types")
                && req.get_ref().query_mode == QueryMode::Normal as i32
        })
        .times(2)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            let sql = request.sql;
            debug!("MockSpanner (Batch Exec): Received execute_sql: {}", sql);
            assert_eq!(
                request.transaction.unwrap().selector.unwrap(),
                TxSelector::Id(b"tx_rw_2".to_vec())
            );
            let mut stats = ResultSetStats::default();
            stats.row_count = Some(RowCount::RowCountExact(1));

            let mut rs = mock_spanner::google::spanner::v1::ResultSet::default();
            rs.stats = Some(stats);
            Ok(tonic::Response::new(rs))
        });

    // 6. Mock commit
    mock_service
        .expect_commit()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            match request.transaction {
                Some(
                    mock_spanner::google::spanner::v1::commit_request::Transaction::TransactionId(
                        id,
                    ),
                ) => {
                    assert_eq!(id, b"tx_rw_2".to_vec());
                }
                _ => panic!("expected TransactionId"),
            }
            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::CommitResponse::default(),
            ))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (mut client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to PGAdapter server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!("Postgres client driver thread error: {}", error);
        }
    });

    let tx = client
        .transaction()
        .await
        .expect("failed to start Postgres transaction");

    // Execute batch DMLs concurrently (pipelining)
    let p1 = tx.execute("INSERT INTO all_types (col_bool) VALUES ($1)", &[&true]);
    let p2 = tx.execute("INSERT INTO all_types (col_bool) VALUES ($1)", &[&false]);

    let (res1, res2) = tokio::try_join!(p1, p2).expect("batch DML execution failed");
    assert_eq!(res1, 1);
    assert_eq!(res2, 1);

    tx.commit().await.expect("commit failed");
}

#[tokio::test]
async fn test_mock_spanner_pipelined_dml() {
    common::init_test();
    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock begin_transaction
    mock_service
        .expect_begin_transaction()
        .withf(|req| {
            let options = req.get_ref().options.as_ref().unwrap();
            options.mode
                == Some(
                    mock_spanner::google::spanner::v1::transaction_options::Mode::ReadWrite(
                        mock_spanner::google::spanner::v1::transaction_options::ReadWrite::default(
                        ),
                    ),
                )
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut tx = Transaction::default();
            tx.id = b"tx_rw_pipelined".to_vec();
            Ok(tonic::Response::new(tx))
        });

    // 4. Mock execute_batch_dml (Assert that BOTH INSERT statements are executed in a SINGLE batch of size 2!)
    mock_service
        .expect_execute_batch_dml()
        .withf(|req| {
            let request = req.get_ref();
            let has_both = request.statements.len() == 2
                && request.statements[0].sql.contains("INSERT INTO all_types")
                && request.statements[1].sql.contains("INSERT INTO all_types");
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::Id(id) => id == b"tx_rw_pipelined",
                    _ => false,
                })
                .unwrap_or(false);
            has_both && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut stats = ResultSetStats::default();
            stats.row_count = Some(RowCount::RowCountExact(1));

            let mut rs1 = mock_spanner::google::spanner::v1::ResultSet::default();
            rs1.stats = Some(stats.clone());

            let mut rs2 = mock_spanner::google::spanner::v1::ResultSet::default();
            rs2.stats = Some(stats);

            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::ExecuteBatchDmlResponse {
                    result_sets: vec![rs1, rs2],
                    status: None,
                    precommit_token: None,
                },
            ))
        });

    // 6. Mock commit
    mock_service
        .expect_commit()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|req| {
            let request = req.into_inner();
            match request.transaction {
                Some(
                    mock_spanner::google::spanner::v1::commit_request::Transaction::TransactionId(
                        id,
                    ),
                ) => {
                    assert_eq!(id, b"tx_rw_pipelined".to_vec());
                }
                _ => panic!("expected TransactionId"),
            }
            Ok(tonic::Response::new(
                mock_spanner::google::spanner::v1::CommitResponse::default(),
            ))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    // Connect using raw TcpStream to guarantee query pipelining
    use bytes::{BufMut, BytesMut};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", pg_port))
        .await
        .expect("failed to connect raw socket");

    // 1. Send StartupMessage
    let mut payload = BytesMut::new();
    payload.put_i32(196608); // version 3.0
    payload.put_slice(b"user\0postgres\0database\0d\0\0");
    let len = (payload.len() + 4) as i32;
    let mut buffer = BytesMut::new();
    buffer.put_i32(len);
    buffer.put_slice(&payload);
    stream.write_all(&buffer).await.unwrap();

    // 2. Read handshake responses until ReadyForQuery status is b'I' (Idle)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');

    // Helper functions to construct messages
    fn encode_parse(query: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'P');
        let query_bytes = query.as_bytes();
        let len = 4 + 1 + query_bytes.len() + 1 + 2;
        buf.extend_from_slice(&(len as i32).to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(query_bytes);
        buf.push(0);
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf
    }

    fn encode_bind_empty() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'B');
        let len = 4 + 1 + 1 + 2 + 2 + 2;
        buf.extend_from_slice(&(len as i32).to_be_bytes());
        buf.push(0);
        buf.push(0);
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf
    }

    fn encode_bind_bool(val: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'B');
        let len = 4 + 1 + 1 + 2 + 2 + 2 + 4 + 1 + 2 + 2;
        buf.extend_from_slice(&(len as i32).to_be_bytes());
        buf.push(0);
        buf.push(0);
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.extend_from_slice(&1i32.to_be_bytes());
        buf.push(if val { 1 } else { 0 });
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.extend_from_slice(&1i16.to_be_bytes());
        buf
    }

    fn encode_execute() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'E');
        let len = 4 + 1 + 4;
        buf.extend_from_slice(&(len as i32).to_be_bytes());
        buf.push(0);
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf
    }

    fn encode_sync() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'S');
        buf.extend_from_slice(&4i32.to_be_bytes());
        buf
    }

    // 3. Construct the entire pipelined sequence
    let mut pipeline = Vec::new();
    pipeline.extend(encode_parse("BEGIN"));
    pipeline.extend(encode_bind_empty());
    pipeline.extend(encode_execute());

    pipeline.extend(encode_parse("INSERT INTO all_types (col_bool) VALUES ($1)"));
    pipeline.extend(encode_bind_bool(true));
    pipeline.extend(encode_execute());

    pipeline.extend(encode_parse("INSERT INTO all_types (col_bool) VALUES ($1)"));
    pipeline.extend(encode_bind_bool(false));
    pipeline.extend(encode_execute());

    pipeline.extend(encode_parse("COMMIT"));
    pipeline.extend(encode_bind_empty());
    pipeline.extend(encode_execute());

    pipeline.extend(encode_sync());

    // Send the entire pipeline in ONE single write
    stream
        .write_all(&pipeline)
        .await
        .expect("failed to write pipelined bytes");

    // Read response until ReadyForQuery to verify successful execution
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');
}

#[tokio::test]
async fn test_mock_spanner_simple_query_implicit_transaction() {
    common::init_test();
    let mut mock_service = mock_spanner::MockSpanner::new();
    let mut seq = mockall::Sequence::new();

    // 1. Mock session creation
    mock_service
        .expect_create_session()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let mut s = Session::default();
            s.name = "projects/p/instances/i/databases/d/sessions/s1".to_string();
            Ok(tonic::Response::new(s))
        });

    // 2. Mock execute_streaming_sql (Dialect detection query)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| req.get_ref().sql.contains("database_dialect"))
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs =
                make_string_partial_result_set(vec!["option_value"], vec![vec!["POSTGRESQL"]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 3. Mock execute_streaming_sql (First statement: SELECT 1 - Execute/Normal)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let is_normal = request.query_mode == 0; // NORMAL
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::Begin(opts) => {
                        opts.mode.as_ref().map(|m| match m {
                            mock_spanner::google::spanner::v1::transaction_options::Mode::ReadOnly(_) => true,
                            _ => false,
                        }).unwrap_or(false)
                    }
                    _ => false,
                })
                .unwrap_or(false);
            request.sql.contains("SELECT 1") && is_normal && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let mut prs = make_int64_partial_result_set(vec!["?column?"], vec![vec![1]]);
            let mut metadata = prs.metadata.clone().unwrap_or_default();
            metadata.transaction = Some(Transaction {
                id: b"tx_implicit_id".to_vec(),
                read_timestamp: None,
                precommit_token: None,
                cache_update: None,
            });
            prs.metadata = Some(metadata);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    // 4. Mock execute_streaming_sql (Second statement: SELECT 2 - Execute/Normal)
    mock_service
        .expect_execute_streaming_sql()
        .withf(|req| {
            let request = req.get_ref();
            let is_normal = request.query_mode == 0; // NORMAL
            let uses_correct_tx = request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref())
                .map(|s| match s {
                    TxSelector::Id(id) => id == b"tx_implicit_id",
                    _ => false,
                })
                .unwrap_or(false);
            request.sql.contains("SELECT 2") && is_normal && uses_correct_tx
        })
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let prs = make_int64_partial_result_set(vec!["?column?"], vec![vec![2]]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(prs)).await;
            });
            Ok(tonic::Response::new(rx))
        });

    let (spanner_uri, _spanner_server) = mock_spanner::start("127.0.0.1:0", mock_service)
        .await
        .expect("failed to start mock Spanner server");

    let server = spanner_pgadapter::server::ProxyServer::bind(
        None,
        None,
        None,
        Some(spanner_uri.clone()),
        0,
    )
    .await
    .expect("failed to bind PGAdapter proxy server");
    let pg_port = server
        .local_addr()
        .expect("failed to get local address")
        .port();
    tokio::spawn(server.start());

    let connection_string = format!(
        "host=127.0.0.1 port={} user=postgres dbname=\"d?useplaintext=true\"",
        pg_port
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    // Run multi-statement simple query
    let results = client
        .simple_query("SELECT 1; SELECT 2;")
        .await
        .expect("failed to execute multi-statement simple query");

    assert_eq!(results.len(), 6); // RowDescription, Row(1), Complete, RowDescription, Row(2), Complete
}
