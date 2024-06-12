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

// [START spanner_data_boost]
import { Client } from 'pg';

async function dataBoost(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  // This enables Data Boost for all partitioned queries on this connection.
  await connection.query("set spanner.data_boost_enabled=true");

  // Run a partitioned query. This query will use Data Boost.
  const singers = await connection.query(
      "run partitioned query "
      + "select singer_id, first_name, last_name "
      + "from singers");
  for (const row of singers.rows) {
    console.log(`${row["singer_id"]} ${row["first_name"]} ${row["last_name"]}`);
  }

  // Close the connection.
  await connection.end();
}
// [END spanner_data_boost]

export = dataBoost;
