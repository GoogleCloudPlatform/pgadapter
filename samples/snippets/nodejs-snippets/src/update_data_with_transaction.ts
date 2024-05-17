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

// [START spanner_dml_getting_started_update]
import { Client } from 'pg';

async function writeWithTransactionUsingDml(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();
  
  // Transfer marketing budget from one album to another. We do it in a
  // transaction to ensure that the transfer is atomic. node-postgres
  // requires you to explicitly start the transaction by executing 'begin'.
  await connection.query("begin");
  const selectMarketingBudgetSql = "SELECT marketing_budget " +
      "from albums " +
      "WHERE singer_id = $1 and album_id = $2";
  // Get the marketing_budget of singer 2 / album 2.
  const album2BudgetResult = await connection.query(selectMarketingBudgetSql, [2, 2]);
  let album2Budget = album2BudgetResult.rows[0]["marketing_budget"];
  const transfer = 200000;
  // The transaction will only be committed if this condition still holds
  // at the time of commit. Otherwise, the transaction will be aborted.
  if (album2Budget >= transfer) {
    // Get the marketing budget of singer 1 / album 1.
    const album1BudgetResult = await connection.query(selectMarketingBudgetSql, [1, 1]);
    let album1Budget = album1BudgetResult.rows[0]["marketing_budget"];
    // Transfer part of the marketing budget of Album 2 to Album 1.
    album1Budget += transfer;
    album2Budget -= transfer;
    const updateSql = "UPDATE albums " +
        "SET marketing_budget = $1 " +
        "WHERE singer_id = $2 and album_id = $3";
    // Start a DML batch. This batch will become part of the current transaction.
    await connection.query("start batch dml");
    // Update the marketing budget of both albums.
    await connection.query(updateSql, [album1Budget, 1, 1]);
    await connection.query(updateSql, [album2Budget, 2, 2]);
    await connection.query("run batch");
  }
  // Commit the current transaction.
  await connection.query("commit");
  console.log("Transferred marketing budget from Album 2 to Album 1");

  // Close the connection.
  await connection.end();
}
// [END spanner_dml_getting_started_update]

export = writeWithTransactionUsingDml;
