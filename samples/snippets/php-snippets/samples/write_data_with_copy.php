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

// [START spanner_copy_from_stdin]
function write_data_with_copy(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    $dir = dirname(__FILE__);
    
    $connection->pgsqlCopyFromFile(
        "singers",
        sprintf("%s/singers_data.txt", $dir),
        "\t",
        "\\\\N",
        "singer_id, first_name, last_name");
    print("Copied 5 singers\n");

    $connection->pgsqlCopyFromFile(
        "albums",
        sprintf("%s/albums_data.txt", $dir));
    print("Copied 5 albums\n");

    $connection = null;
}
// [END spanner_copy_from_stdin]
