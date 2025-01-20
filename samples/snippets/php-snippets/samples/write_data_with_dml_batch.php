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

// [START spanner_dml_batch]
function write_data_with_dml_batch(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Use START BATCH DML / RUN BATCH to run a batch of DML statements.
    // Create a prepared statement for the DML that should be executed.
    $sql = "INSERT INTO singers (singer_id, first_name, last_name) VALUES (?, ?, ?)";
    $statement = $connection->prepare($sql);
    // Start a DML batch.
    $connection->exec("START BATCH DML");
    
    $statement->execute([16, "Sarah", "Wilson"]);
    $statement->execute([17, "Ethan", "Miller"]);
    $statement->execute([18, "Maya", "Patel"]);
    
    // Run the DML batch. Use the 'query(..)' method, as the update counts are returned as a row
    // containing an array with the update count of each statement in the batch.
    $statement = $connection->query("RUN BATCH");
    $result = $statement->fetchAll();
    $update_count = $result[0][0];
    
    printf("%s records inserted\n", $update_count);

    $statement = null;
    $connection = null;
}
// [END spanner_dml_batch]
