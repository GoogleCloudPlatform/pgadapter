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

// [START spanner_copy_from_stdin]
import { Client } from 'pg';
import { pipeline } from 'node:stream/promises'
import fs from 'node:fs'
import { from as copyFrom } from 'pg-copy-streams'
import path from "path";

async function writeDataWithCopy(host: string, port: number, database: string): Promise<void> {
  const connection = new Client({
    host: host,
    port: port,
    database: database,
  });
  await connection.connect();

  // Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
  // will succeed even if it exceeds Spanner's mutation limit per transaction.
  await connection.query("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
  // Copy data from a csv file to Spanner using the COPY command.
  // Note that even though the command says 'from stdin', the actual input comes from a file.
  const copySingersStream = copyFrom('copy singers (singer_id, first_name, last_name) from stdin');
  const ingestSingersStream = connection.query(copySingersStream);
  const sourceSingersStream = fs.createReadStream(path.join(__dirname, 'singers_data.txt'));
  await pipeline(sourceSingersStream, ingestSingersStream);
  console.log(`Copied ${copySingersStream.rowCount} singers`);

  const copyAlbumsStream = copyFrom('copy albums from stdin');
  const ingestAlbumsStream = connection.query(copyAlbumsStream);
  const sourceAlbumsStream = fs.createReadStream(path.join(__dirname, 'albums_data.txt'));
  await pipeline(sourceAlbumsStream, ingestAlbumsStream);
  console.log(`Copied ${copyAlbumsStream.rowCount} albums`);

  // Close the connection.
  await connection.end();
}
// [END spanner_copy_from_stdin]

export = writeDataWithCopy;
