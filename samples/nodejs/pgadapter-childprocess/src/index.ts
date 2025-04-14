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

import {startPGAdapter} from 'test-wrapped-binary'
import { Client } from 'pg';

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function main() {
  const pgAdapter = await startPGAdapter({
    project: "appdev-soda-spanner-staging",
    instance: "knut-test-ycsb",
    port: 5433,
  });
  try {
    //await sleep(500);

    console.log('Started PGAdapter');

    // Execute a simple query.
    const connection = new Client({
      host: "localhost",
      port: 5433,
      database: "knut-test-db",
    });
    await connection.connect();

    const result = await connection.query("SELECT * " +
        "FROM all_types " +
        "LIMIT 10");
    for (const row of result.rows) {
      console.log(JSON.stringify(row));
    }

    // Close the connection.
    await connection.end();
  } finally {
    pgAdapter.kill();
  }
}

(async () => {
  await main();
})().catch(e => {
  console.error(e);
  process.exit(1);
});
