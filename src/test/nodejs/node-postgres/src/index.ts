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

async function testInsertTwice(client) {
  try {
    await client.query('BEGIN');
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res1 = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res1.rowCount} row(s)`);
    const res2 = await client.query(queryText, ['bar']);
    console.log(`Inserted ${res2.rowCount} row(s)`);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAutoCommit(client) {
  try {
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypes(client) {
  try {
    const queryText = 'INSERT INTO AllTypes ' +
        '(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) ' +
        'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';
    const res = await client.query(queryText, [
        1, true, Buffer.from('some random string', 'utf-8'),
        3.14, 100, 234.54235, new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
        '2022-07-22', 'some-random-string', { my_key: "my-value" }]);
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
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
.command(
    'testInsert <port>',
    'Inserts a single row',
    {},
    opts => runTest(opts.port, testInsert)
)
.command(
    'testInsertTwice <port>',
    'Executes the same parameterized insert statement twice',
    {},
    opts => runTest(opts.port, testInsertTwice)
)
.command(
    'testInsertAutoCommit <port>',
    'Inserts a single row using auto commit',
    {},
    opts => runTest(opts.port, testInsertAutoCommit)
)
.command(
    'testInsertAllTypes <port>',
    'Inserts a row using all supported types',
    {},
    opts => runTest(opts.port, testInsertAllTypes)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
