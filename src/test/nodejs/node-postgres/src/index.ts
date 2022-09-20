// Copyright 2022 Google LLC
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

import "reflect-metadata"
const { Client } = require('pg')

function runTest(port: number, test: (client) => Promise<void>) {
  const client = new Client({
    host: "localhost",
    port,
    database: "db",
  })
  client.connect()
    .then(() => {
      test(client).then(() => client.end());
    })
    .catch((error) => console.log(error));
}

async function testSelect1(client) {
  try {
    const {rows} = await client.query('SELECT 1');
    if (rows) {
      console.log(`SELECT 1 returned: ${Object.values(rows[0])[0]}`);
    } else {
      console.error('Could not select 1');
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testInsert(client) {
  try {
    await client.query('BEGIN');
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res.rowCount} row(s)`);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(`Insert error: ${e}`);
  }
}

require('yargs')
.demand(2)
.command(
    'testSelect1 <port>',
    'Executes SELECT 1',
    {},
    opts => runTest(opts.port, testSelect1)
)
.example('node $0 select1 5432')
.command(
    'testInsert <port>',
    'Inserts a single row',
    {},
    opts => runTest(opts.port, testInsert)
)
.example('node $0 select1 5432')
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
