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

// [START spanner_dml_getting_started_insert]
function write_data_with_dml(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    $sql = "INSERT INTO singers (singer_id, first_name, last_name)"
                        ." VALUES (?, ?, ?), (?, ?, ?), "
                        ."        (?, ?, ?), (?, ?, ?)";
    $statement = $connection->prepare($sql);
    $statement->execute([
        12, "Melissa", "Garcia",
        13, "Russel", "Morales",
        14, "Jacqueline", "Long",
        15, "Dylan", "Shaw"
    ]);
    printf("%d records inserted\n", $statement->rowCount());

    $statement = null;
    $connection = null;
}
// [END spanner_dml_getting_started_insert]
