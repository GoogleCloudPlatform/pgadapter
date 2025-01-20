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

// [START spanner_read_only_transaction]
function read_only_transaction(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    // Start a transaction.
    $connection->beginTransaction();
    // Change the current transaction to a read-only transaction.
    // This statement can only be executed at the start of a transaction.
    $connection->exec("set transaction read only");
    
    // The following two queries use the same read-only transaction.
    $statement = $connection->query(
        "select singer_id, album_id, album_title "
        ."from albums "
        ."order by singer_id, album_id"
    );
    $rows = $statement->fetchAll();
    foreach ($rows as $album)
    {
        printf("%s\t%s\t%s\n", $album["singer_id"], $album["album_id"], $album["album_title"]);
    }

    $statement = $connection->query(
        "select singer_id, album_id, album_title "
        ."from albums "
        ."order by album_title"
    );
    $rows = $statement->fetchAll();
    foreach ($rows as $album)
    {
        printf("%s\t%s\t%s\n", $album["singer_id"], $album["album_id"], $album["album_title"]);
    }

    # Read-only transactions must also be committed or rolled back to mark
    # the end of the transaction. There is no semantic difference between
    # rolling back or committing a read-only transaction.
    $connection->commit();

    $rows = null;
    $statement = null;
    $connection = null;
}
// [END spanner_read_only_transaction]
