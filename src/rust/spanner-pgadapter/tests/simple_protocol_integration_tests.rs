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

use tokio_postgres::{NoTls, SimpleQueryMessage};

mod common;

#[tokio::test]
async fn test_simple_query_select_1() {
    let port = common::start_test_server().await;

    // Connect to the loopback test server
    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    // Spawn the connection driver to process connection events in the background
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    // Execute simple query
    let results = client
        .simple_query("SELECT 1")
        .await
        .expect("failed to execute simple query");

    assert_eq!(results.len(), 3); // RowDescription + Row + CommandComplete

    match &results[0] {
        SimpleQueryMessage::RowDescription(columns) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].name(), "?column?");
        }
        other => panic!("expected RowDescription, got {:?}", other),
    }

    match &results[1] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.len(), 1);
            assert_eq!(row.get(0), Some("1"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[2] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 1);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }
}

#[tokio::test]
async fn test_simple_query_select_rows() {
    let port = common::start_test_server().await;

    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    let results = client
        .simple_query("SELECT id, name FROM users;")
        .await
        .expect("failed to execute select query");

    assert_eq!(results.len(), 4); // RowDescription + 2 Rows + CommandComplete

    match &results[0] {
        SimpleQueryMessage::RowDescription(columns) => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name(), "id");
            assert_eq!(columns[1].name(), "name");
        }
        other => panic!("expected RowDescription, got {:?}", other),
    }

    match &results[1] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.len(), 2);
            assert_eq!(row.get(0), Some("1"));
            assert_eq!(row.get(1), Some("Alice"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[2] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.len(), 2);
            assert_eq!(row.get(0), Some("2"));
            assert_eq!(row.get(1), Some("Bob"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[3] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 2);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }
}

#[tokio::test]
async fn test_simple_query_command_complete() {
    let port = common::start_test_server().await;

    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    let results = client
        .simple_query("INSERT INTO users (id, name) VALUES (3, 'Charlie');")
        .await
        .expect("failed to execute insert command");

    assert_eq!(results.len(), 1); // Only CommandComplete
    match &results[0] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 1);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }
}

#[tokio::test]
async fn test_transaction_status_transitions() {
    use bytes::{BufMut, BytesMut};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let port = common::start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to connect");

    // 1. Send StartupMessage
    let mut payload = BytesMut::new();
    payload.put_i32(196608); // version 3.0
    payload.put_slice(b"user\0postgres\0database\0test\0\0");
    let len = (payload.len() + 4) as i32;
    let mut buffer = BytesMut::new();
    buffer.put_i32(len);
    buffer.put_slice(&payload);
    stream.write_all(&buffer).await.unwrap();

    // 2. Read handshake response, check ReadyForQuery status is b'I' (Idle)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');

    // 3. Send "BEGIN" simple query
    let sql = "BEGIN\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();

    // 4. Read response, check ReadyForQuery status is b'T' (In Transaction)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'T');

    // 5. Send "COMMIT" simple query
    let sql = "COMMIT\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();

    // 6. Read response, check ReadyForQuery status transitions back to b'I' (Idle)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');
}

#[tokio::test]
async fn test_transaction_multiple_begin() {
    use bytes::{BufMut, BytesMut};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let port = common::start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to connect");

    // 1. Startup
    let mut payload = BytesMut::new();
    payload.put_i32(196608);
    payload.put_slice(b"user\0postgres\0database\0test\0\0");
    let len = (payload.len() + 4) as i32;
    let mut buffer = BytesMut::new();
    buffer.put_i32(len);
    buffer.put_slice(&payload);
    stream.write_all(&buffer).await.unwrap();

    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');

    // 2. Send "BEGIN"
    let sql = "BEGIN\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'T');

    // 3. Send second "BEGIN" (should be a no-op/ignored, and still be in transaction)
    let sql = "BEGIN\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'T');

    // 4. Send "COMMIT"
    let sql = "COMMIT\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');
}

#[tokio::test]
async fn test_transaction_implicit_to_explicit() {
    use bytes::{BufMut, BytesMut};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let port = common::start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to connect");

    // 1. Startup
    let mut payload = BytesMut::new();
    payload.put_i32(196608);
    payload.put_slice(b"user\0postgres\0database\0test\0\0");
    let len = (payload.len() + 4) as i32;
    let mut buffer = BytesMut::new();
    buffer.put_i32(len);
    buffer.put_slice(&payload);
    stream.write_all(&buffer).await.unwrap();

    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');

    // 2. Send simple query with "SELECT 1; BEGIN; SELECT 2;"
    // This will start an implicit transaction (on SELECT 1),
    // then transition it to explicit (on BEGIN),
    // then execute SELECT 2 in it.
    // Sync at the end should NOT commit it because it's explicit.
    let sql = "SELECT 1; BEGIN; SELECT 2;\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();

    // The ReadyForQuery status byte should be 'T' (In Transaction Block)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'T');

    // 3. Send "COMMIT" to explicitly commit it
    let sql = "COMMIT\0";
    let mut query_packet = BytesMut::new();
    query_packet.put_u8(b'Q');
    query_packet.put_i32((sql.len() + 4) as i32);
    query_packet.put_slice(sql.as_bytes());
    stream.write_all(&query_packet).await.unwrap();

    // The ReadyForQuery status byte should go back to 'I' (Idle)
    let status = common::read_until_ready(&mut stream).await;
    assert_eq!(status, b'I');
}

#[tokio::test]
async fn test_simple_query_multiple_selects() {
    let port = common::start_test_server().await;
    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    let results = client
        .simple_query("SELECT 1; SELECT id, name FROM users;")
        .await
        .expect("failed to execute multiple selects");

    // We expect:
    // 1. SELECT 1 row description
    // 2. SELECT 1 row (1)
    // 3. SELECT 1 command complete
    // 4. users description
    // 5. users row 1
    // 6. users row 2
    // 7. users command complete
    assert_eq!(results.len(), 7);

    match &results[0] {
        SimpleQueryMessage::RowDescription(cols) => {
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0].name(), "?column?");
        }
        other => panic!("expected RowDescription, got {:?}", other),
    }

    match &results[1] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("1"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[2] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 1);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }

    match &results[3] {
        SimpleQueryMessage::RowDescription(cols) => {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].name(), "id");
            assert_eq!(cols[1].name(), "name");
        }
        other => panic!("expected RowDescription, got {:?}", other),
    }

    match &results[4] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(1), Some("Alice"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[5] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(1), Some("Bob"));
        }
        other => panic!("expected Row, got {:?}", other),
    }

    match &results[6] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 2);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }
}

#[tokio::test]
async fn test_simple_query_empty_statement() {
    let port = common::start_test_server().await;
    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    let results = client
        .simple_query(";;   ;;")
        .await
        .expect("failed to execute empty query");

    assert_eq!(results.len(), 1);
    match &results[0] {
        SimpleQueryMessage::CommandComplete(rows) => {
            assert_eq!(*rows, 0);
        }
        other => panic!("expected CommandComplete(0), got {:?}", other),
    }
}

#[tokio::test]
async fn test_simple_query_halt_on_error() {
    let port = common::start_test_server().await;
    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    // We execute a batch where the second statement fails
    let err = client
        .simple_query("SELECT 1; SELECT_FAIL; SELECT id, name FROM users;")
        .await
        .expect_err("expected error during execution of batch");

    let db_err = err.as_db_error().expect("expected DbError");
    assert_eq!(db_err.message(), "mock query execution error");
}

#[tokio::test]
async fn test_simple_query_mixed_batch() {
    let port = common::start_test_server().await;
    let connection_string = format!("host=127.0.0.1 port={} user=postgres dbname=test", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("failed to connect to test server");

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {}", error);
        }
    });

    let results = client
        .simple_query("INSERT INTO users (id, name) VALUES (4, 'Dave'); SELECT 1;")
        .await
        .expect("failed to execute mixed batch");

    // Expecting:
    // 1. CommandComplete (insert)
    // 2. RowDescription (SELECT 1)
    // 3. Row (1)
    // 4. CommandComplete (SELECT 1)
    assert_eq!(results.len(), 4);

    match &results[0] {
        SimpleQueryMessage::CommandComplete(rows_affected) => {
            assert_eq!(*rows_affected, 1);
        }
        other => panic!("expected CommandComplete, got {:?}", other),
    }

    match &results[2] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("1"));
        }
        other => panic!("expected Row, got {:?}", other),
    }
}
