<?php

// Copyright 2025 Google LLC
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
function update_data_with_transaction(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    // Start a read/write transaction.
    $connection->beginTransaction();
    // Transfer marketing budget from one album to another.
    // We do it in a transaction to ensure that the transfer is atomic.
    
    // Create a prepared statement that we can use to execute the same
    // SQL string multiple times with different parameter values.
    $select_marketing_budget_statement = $connection->prepare(
        "SELECT marketing_budget "
        ."from albums "
        ."WHERE singer_id = ? "
        ."and album_id = ?"
    );
    // Get the marketing budget of Album #2.
    $select_marketing_budget_statement->execute([2, 2]);
    $album2_budget = $select_marketing_budget_statement->fetchAll()[0][0];
    $select_marketing_budget_statement->closeCursor();
        
    $transfer = 200000;
    if ($album2_budget > $transfer) {
        // Get the marketing budget of Album #1.
        $select_marketing_budget_statement->execute([1, 1]);
        $album1_budget = $select_marketing_budget_statement->fetchAll()[0][0];
        $select_marketing_budget_statement->closeCursor();
        // Transfer the marketing budgets and write the update back
        // to the database.
        $album1_budget += $transfer;
        $album2_budget -= $transfer;
        // PHP PDO also supports named query parameters.
        $update_statement = $connection->prepare(
            "update albums "
                ."set marketing_budget = :budget "
                ."where singer_id = :singer_id "
                ."and   album_id = :album_id"
        );
        // Start a DML batch. This batch will become part of the current transaction.
        // $connection->exec("start batch dml");
        // Update the marketing budget of both albums.
        $update_statement->execute(["budget" => $album1_budget, "singer_id" => 1, "album_id" => 1]);
        $update_statement->execute(["budget" => $album2_budget, "singer_id" => 2, "album_id" => 2]);
        // $connection->exec("run batch");
    } else {
        print("Insufficient budget to transfer\n");
    }
    // Commit the transaction.
    $connection->commit();
    print("Transferred marketing budget from Album 2 to Album 1\n");

    $connection = null;
}
// [END spanner_dml_getting_started_update]
