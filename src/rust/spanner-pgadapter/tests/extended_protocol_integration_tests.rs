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

use tokio_postgres::NoTls;

mod common;

#[tokio::test]
async fn test_extended_query_prepare_and_execute() {
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

    // 1. Prepare statement (sends Parse & Describe statement)
    let statement = client
        .prepare("SELECT id, name FROM users")
        .await
        .expect("failed to prepare statement");

    assert_eq!(statement.params().len(), 0);
    assert_eq!(statement.columns().len(), 2);
    assert_eq!(statement.columns()[0].name(), "id");
    assert_eq!(statement.columns()[1].name(), "name");

    // 2. Execute statement (sends Bind & Execute & Sync)
    let rows = client
        .query(&statement, &[])
        .await
        .expect("failed to execute prepared statement");

    assert_eq!(rows.len(), 2);
    let id1: i32 = rows[0].get(0);
    let name1: &str = rows[0].get(1);
    assert_eq!(id1, 1);
    assert_eq!(name1, "Alice");

    let id2: i32 = rows[1].get(0);
    let name2: &str = rows[1].get(1);
    assert_eq!(id2, 2);
    assert_eq!(name2, "Bob");
}

#[tokio::test]
async fn test_extended_query_with_parameters() {
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

    // Prepare statement with one parameter (sends Parse & Describe statement)
    let statement = client
        .prepare("SELECT id, name FROM users WHERE id = $1")
        .await
        .expect("failed to prepare statement");

    assert_eq!(statement.params().len(), 1);
    assert_eq!(statement.columns().len(), 2);

    // Execute with binding parameters (sends Bind & Execute & Sync)
    let rows = client
        .query(&statement, &[&1i32])
        .await
        .expect("failed to execute prepared statement");

    // Our mockup MockBackend currently returns the same hardcoded list of users
    // regardless of the bound parameter value.
    assert_eq!(rows.len(), 2);
    let id1: i32 = rows[0].get(0);
    assert_eq!(id1, 1);
}
