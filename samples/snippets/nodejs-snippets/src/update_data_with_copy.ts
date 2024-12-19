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

// [START spanner_update_data]
import { Client } from 'pg';
import { pipeline } from 'node:stream/promises'
import { from as copyFrom } from 'pg-copy-streams'
import {Readable} from "stream";

async function updateDataWithCopy(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  // Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
  // will succeed even if it exceeds Spanner's mutation limit per transaction.
  await connection.query("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
  
  // Instruct PGAdapter to use insert-or-update for COPY statements.
  // This enables us to use COPY to update existing data.
  await connection.query("set spanner.copy_upsert=true");
  
  // Copy data to Spanner using the COPY command.
  const copyStream = copyFrom('COPY albums (singer_id, album_id, marketing_budget) FROM STDIN');
  const ingestStream = connection.query(copyStream);

  // Create a source stream and attach the source to the destination.
  const sourceStream = new Readable();
  const operation = pipeline(sourceStream, ingestStream);
  // Manually push data to the source stream to write data to Spanner.
  sourceStream.push("1\t1\t100000\n");
  sourceStream.push("2\t2\t500000\n");
  // Push a 'null' to indicate the end of the stream.
  sourceStream.push(null);
  // Wait for the copy operation to finish.
  await operation;
  console.log(`Updated ${copyStream.rowCount} albums`);

  // Close the connection.
  await connection.end();
}
// [END spanner_update_data]

export = updateDataWithCopy;
