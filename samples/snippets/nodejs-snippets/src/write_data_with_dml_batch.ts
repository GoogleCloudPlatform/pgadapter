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

// [START spanner_dml_batch]
import { Client } from 'pg';

async function writeDataWithDmlBatch(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();
  
  // node-postgres does not support PostgreSQL pipeline mode, so we must use the
  // `start batch dml` / `run batch` statements to execute a DML batch.
  const sql = "INSERT INTO singers (singer_id, first_name, last_name) VALUES ($1, $2, $3)";
  await connection.query("start batch dml");
  await connection.query(sql, [16, "Sarah", "Wilson"]);
  await connection.query(sql, [17, "Ethan", "Miller"]);
  await connection.query(sql, [18, "Maya", "Patel"]);
  const result = await connection.query("run batch");
  // RUN BATCH returns the update counts as an array of strings, with one element for each
  // DML statement in the batch. This calculates the total number of affected rows from that array.
  const updateCount = result.rows[0]["UPDATE_COUNTS"]
      .map((s: string) => parseInt(s))
      .reduce((c: number, current: number) => c + current, 0);
  console.log(`${updateCount} records inserted`);

  // Close the connection.
  await connection.end();
}
// [END spanner_dml_batch]

export = writeDataWithDmlBatch;
