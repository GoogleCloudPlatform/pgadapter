// Copyright 2024 Google LLC
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
import { Client } from 'pg';

async function createTables(host: string, port: number, database: string): Promise<void> {
  // Connect to Spanner through PGAdapter.
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  // Create two tables in one batch.
  await connection.query("START BATCH DDL");
  await connection.query("CREATE TABLE Singers ("
      + "  SingerId   bigint NOT NULL,"
      + "  FirstName  character varying(1024),"
      + "  LastName   character varying(1024),"
      + "  SingerInfo bytea,"
      + "  FullName character varying(2048) GENERATED "
      + "  ALWAYS AS (FirstName || ' ' || LastName) STORED,"
      + "  PRIMARY KEY (SingerId)"
      + ")");
  await connection.query("CREATE TABLE Albums ("
      + "  SingerId     bigint NOT NULL,"
      + "  AlbumId      bigint NOT NULL,"
      + "  AlbumTitle   character varying(1024),"
      + "  PRIMARY KEY (SingerId, AlbumId)"
      + ") INTERLEAVE IN PARENT Singers ON DELETE CASCADE");
  await connection.query("RUN BATCH");
  console.log(`Created Singers & Albums tables in database: [${database}]`);

  // Close the connection.
  await connection.end();
}
// [END spanner_create_database]

export = createTables;
