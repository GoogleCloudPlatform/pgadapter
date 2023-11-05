// Copyright 2023 Google LLC
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

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>) {
  const knex = require('knex')({
    client: 'pg',
    connection: {
      host: host,
      port: port,
      database: database,
      ssl: false,
    }
  });
  runTestWithClient(knex, test);
}

function runTestWithClient(client, test: (client) => Promise<void>) {
  test(client).then(() => client.destroy());
}

async function testSelect1(client) {
  try {
    const rows = await client.select('*').fromRaw('(select 1)');
    if (rows) {
      console.log(`SELECT 1 returned: ${Object.values(rows[0])[0]}`);
    } else {
      console.error('Could not select 1');
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testSelectUser(client) {
  try {
    const user = await client('users').where('id', 1).first();
    console.log(user);
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

require('yargs')
.demand(4)
.command(
    'testSelect1 <host> <port> <database>',
    'Executes SELECT 1',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelect1)
)
.command(
    'testSelectUser <host> <port> <database>',
    'Selects a user',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectUser)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
