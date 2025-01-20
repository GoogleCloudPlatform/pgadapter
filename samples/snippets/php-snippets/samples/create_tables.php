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

// [START spanner_create_database]
function create_tables(string $host, string $port, string $database): void
{
    // Connect to Spanner through PGAdapter using the PostgreSQL PDO driver.
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);
    
    // Create two tables in one batch.
    $connection->exec("start batch ddl");
    $connection->exec("create table singers ("
        ."  singer_id   bigint primary key not null,"
        ."  first_name  character varying(1024),"
        ."  last_name   character varying(1024),"
        ."  singer_info bytea,"
        ."  full_name   character varying(2048) generated "
        ."  always as (first_name || ' ' || last_name) stored"
        .")");
    $connection->exec("create table albums ("
        ."  singer_id     bigint not null,"
        ."  album_id      bigint not null,"
        ."  album_title   character varying(1024),"
        ."  primary key (singer_id, album_id)"
        .") interleave in parent singers on delete cascade");
    $connection->exec("run batch");
    print("Created Singers & Albums tables in database: [{$database}]\n");

    $connection = null;
}
// [END spanner_create_database]
