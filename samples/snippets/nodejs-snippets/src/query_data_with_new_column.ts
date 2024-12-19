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

// [START spanner_query_data_with_new_column]
import { Client } from 'pg';

async function queryDataWithNewColumn(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  const result = await connection.query(
      "SELECT singer_id, album_id, marketing_budget "
      + "FROM albums "
      + "ORDER BY singer_id, album_id"
  );
  for (const row of result.rows) {
    console.log(`${row["singer_id"]} ${row["album_id"]} ${row["marketing_budget"]}`);
  }

  // Close the connection.
  await connection.end();
}
// [END spanner_query_data_with_new_column]

export = queryDataWithNewColumn;
