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

// [START spanner_partitioned_dml]
import { Client } from 'pg';

async function partitionedDml(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  // Enable Partitioned DML on this connection.
  await connection.query("set spanner.autocommit_dml_mode='partitioned_non_atomic'");

  // Back-fill a default value for the MarketingBudget column.
  const lowerBoundUpdateCount = await connection.query(
      "update albums " +
      "set marketing_budget=0 " +
      "where marketing_budget is null");
  console.log(`Updated at least ${lowerBoundUpdateCount.rowCount} albums`);

  // Close the connection.
  await connection.end();
}
// [END spanner_partitioned_dml]

export = partitionedDml;
