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

// [START spanner_partitioned_dml]
function execute_partitioned_dml(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Change the DML mode that is used by this connection to Partitioned
    // DML. Partitioned DML is designed for bulk updates and deletes.
    // See https://cloud.google.com/spanner/docs/dml-partitioned for more
    // information.
    $connection->exec("set spanner.autocommit_dml_mode='partitioned_non_atomic'");

    // The following statement will use Partitioned DML.
    $rowcount = $connection->exec(
        "update albums "
        ."set marketing_budget=0 "
        ."where marketing_budget is null"
    );
    printf("Updated at least %d albums\n", $rowcount);

    $statement = null;
    $connection = null;
}
// [END spanner_partitioned_dml]
