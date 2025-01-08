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

// [START spanner_transaction_and_statement_tag]
function tags(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    // Start a read/write transaction.
    $connection->beginTransaction();
    
    // Set the TRANSACTION_TAG session variable to set a transaction tag
    // for the current transaction.
    $connection->exec("set spanner.transaction_TAG='example-tx-tag'");

    // Set the STATEMENT_TAG session variable to set the request tag
    // that should be included with the next SQL statement.
    $connection->exec("set spanner.statement_tag='query-marketing-budget'");

    $singer_id = 1;
    $album_id = 1;
    $statement = $connection->prepare(
        "select marketing_budget "
        ."from albums "
        ."where singer_id = ? "
        ."  and album_id  = ?"
    );
    $statement->execute([1, 1]);
    $marketing_budget = $statement->fetchAll()[0][0];
    $statement->closeCursor();

    # Reduce the marketing budget by 10% if it is more than 1,000.
    $max_marketing_budget = 1000;
    $reduction = 0.1;
    if ($marketing_budget > $max_marketing_budget) {
        // Make sure the marketing_budget remains an int.
        $marketing_budget -= intval($marketing_budget * $reduction);
        // Set a statement tag for the update statement.
        $connection->exec("set spanner.statement_tag='reduce-marketing-budget'");
        $update_statement = $connection->prepare(
            "update albums set marketing_budget = :budget "
            ."where singer_id = :singer_id "
            ."  and album_id  = :album_id"
        );
        $update_statement->execute([
            "budget" => $marketing_budget,
            "singer_id" => $singer_id,
            "album_id" => $album_id,
        ]);
    } else {
        print("Marketing budget already less than or equal to 1,000\n");
    }
    // Commit the transaction.
    $connection->commit();
    print("Reduced marketing budget\n");

    $connection = null;
}
// [END spanner_transaction_and_statement_tag]
