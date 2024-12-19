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

// [START spanner_transaction_and_statement_tag]
import { Client } from 'pg';

async function tags(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  await connection.query("begin");
  // Set the TRANSACTION_TAG session variable to set a transaction tag
  // for the current transaction.
  await connection.query("set spanner.transaction_tag='example-tx-tag'");
  // Set the STATEMENT_TAG session variable to set the request tag
  // that should be included with the next SQL statement.
  await connection.query("set spanner.statement_tag='query-marketing-budget'");
  const budgetResult = await connection.query(
      "select marketing_budget " +
      "from albums " +
      "where singer_id=$1 and album_id=$2", [1, 1])
  let budget = budgetResult.rows[0]["marketing_budget"];
  // Reduce the marketing budget by 10% if it is more than 1,000.
  if (budget > 1000) {
    budget = budget - budget * 0.1;
    await connection.query("set spanner.statement_tag='reduce-marketing-budget'");
    await connection.query("update albums set marketing_budget=$1 "
        + "where singer_id=$2 AND album_id=$3", [budget, 1, 1]);
  }
  // Commit the current transaction.
  await connection.query("commit");
  console.log("Reduced marketing budget");

  // Close the connection.
  await connection.end();
}
// [END spanner_transaction_and_statement_tag]

export = tags;
