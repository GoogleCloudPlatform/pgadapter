// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START spanner_read_only_transaction]
import { Client } from 'pg';

async function readOnlyTransaction(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();
  
  // Start a transaction.
  await connection.query("begin");
  // This SQL statement instructs the PGAdapter to make it a read-only transaction.
  await connection.query("set transaction read only");

  const albumsOrderById = await connection.query(
      "SELECT singer_id, album_id, album_title "
      + "FROM albums "
      + "ORDER BY singer_id, album_id");
  for (const row of albumsOrderById.rows) {
    console.log(`${row["singer_id"]} ${row["album_id"]} ${row["album_title"]}`);
  }
  const albumsOrderByTitle = await connection.query(
      "SELECT singer_id, album_id, album_title "
      + "FROM albums "
      + "ORDER BY album_title");
  for (const row of albumsOrderByTitle.rows) {
    console.log(`${row["singer_id"]} ${row["album_id"]} ${row["album_title"]}`);
  }
  // End the read-only transaction by executing commit.
  await connection.query("commit");

  // Close the connection.
  await connection.end();
}
// [END spanner_read_only_transaction]

export = readOnlyTransaction;
