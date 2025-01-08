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

// [START spanner_query_with_parameter]
function query_data_with_parameter(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    $statement = $connection->prepare("SELECT singer_id, first_name, last_name "
                        ."FROM singers "
                        ."WHERE last_name = ?"
    );
    $statement->execute(["Garcia"]);
    $rows = $statement->fetchAll();
    foreach ($rows as $singer)
    {
        printf("%s\t%s\t%s\n", $singer["singer_id"], $singer["first_name"], $singer["last_name"]);
    }

    $rows = null;
    $statement = null;
    $connection = null;
}
// [END spanner_query_with_parameter]
