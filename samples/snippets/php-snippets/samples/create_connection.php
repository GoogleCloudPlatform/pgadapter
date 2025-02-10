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

// [START spanner_create_connection]
function create_connection(string $host, string $port, string $database): void
{
    // Connect to Spanner through PGAdapter using the PostgreSQL PDO driver.
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Execute a query on Spanner through PGAdapter.
    $statement = $connection->query("select 'Hello world!' as hello");
    $rows = $statement->fetchAll();

    printf("Greeting from Cloud Spanner PostgreSQL: %s\n", $rows[0][0]);

    // Cleanup resources.
    $rows = null;
    $statement = null;
    $connection = null;
}
// [END spanner_create_connection]
