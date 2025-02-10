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

// [START spanner_statement_timeout]
function query_with_timeout(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Set the statement timeout that should be used for all statements
    // on this connection to 5 seconds.
    // Supported time units are 's' (seconds), 'ms' (milliseconds),
    // 'us' (microseconds), and 'ns' (nanoseconds).
    $connection->exec("set statement_timeout='5s'");

    try {
        $statement = $connection->query("SELECT singer_id, album_id, album_title "
            . "FROM albums "
            . "WHERE album_title in ("
            . "  SELECT first_name "
            . "  FROM singers "
            . "  WHERE last_name LIKE '%a%'"
            . "     OR last_name LIKE '%m%'"
            . ")");
        $rows = $statement->fetchAll();
        foreach ($rows as $album) {
            printf("%s\t%s\t%s\n", $album["singer_id"], $album["album_id"], $album["album_title"]);
        }
    } catch (Exception $exception) {
        printf("Error occurred during query execution: %s", $exception->getMessage());
    }

    $rows = null;
    $statement = null;
    $connection = null;
}
// [END spanner_statement_timeout]
