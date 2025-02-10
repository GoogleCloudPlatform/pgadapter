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

// [START spanner_data_boost]
function data_boost(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // This enables Data Boost for all partitioned queries on this
    // connection.
    $connection->exec("set spanner.data_boost_enabled=true");

    // Run a partitioned query. This query will use Data Boost.
    $statement = $connection->query(
        "run partitioned query "
        ."select singer_id, first_name, last_name "
        ."from singers"
    );
    $rows = $statement->fetchAll();
    foreach ($rows as $singer) {
        printf("%s\t%s\t%s\n", $singer["singer_id"], $singer["first_name"], $singer["last_name"]);
    }

    $rows = null;
    $statement = null;
    $connection = null;
}
// [END spanner_data_boost]
