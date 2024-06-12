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

import {Knex} from "knex";

interface User {
  id: number;
  name: string;
  age: number;
}

interface AllTypes {
  col_bigint: number;
  col_bool: boolean;
  col_bytea: Buffer;
  col_float4: number;
  col_float8: number;
  col_int: number;
  col_numeric: string;
  col_timestamptz: Date;
  col_date: string;
  col_varchar: string;
  col_jsonb: any;
}

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>) {
  const knex = require('knex')({
    client: 'pg',
    connection: {
      host: host,
      port: port,
      database: database,
      ssl: false,
      timezone: 'UTC',
    }
  }) as Knex;
  runTestWithClient(knex, test);
}

function runTestWithClient(client: Knex, test: (client: Knex) => Promise<void>) {
  test(client).then(() => client.destroy());
}

async function testSelect1(client: Knex) {
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

async function testSelectUser(client: Knex) {
  try {
    const user = await client<User>('users').where('id', 1).first();
    console.log(user);
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testSelectAllTypes(client: Knex) {
  try {
    const all_types = await client<AllTypes>('all_types').where('col_bigint', 1).first();
    console.log(all_types);
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testInsertAllTypes(client: Knex) {
  try {
    const result = await client<AllTypes>('all_types').insert({
      col_bigint: 1,
      col_bool: true,
      col_bytea: Buffer.from('some random string'),
      col_float4: 3.14,
      col_float8: 3.14,
      col_int: 100,
      col_numeric: '6.626',
      col_timestamptz: new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
      col_date: '2024-06-10',
      col_varchar: 'some random string',
      col_jsonb: {key: 'value'},
    } as AllTypes);
    console.log(result);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testReadWriteTransaction(client: Knex) {
  try {
    const transactionResult = await client.transaction(async trx => {
      const user = await trx<User>('users').where('id', 1).first();
      console.log(user);
      return trx.insert({id: 1, value: 'One'}).into('users');
    });
    console.log(transactionResult);
  } catch (e) {
    console.error(`Transaction error: ${e}`);
  }
}

async function testReadWriteTransactionError(client: Knex) {
  try {
    const transactionResult = await client.transaction(async trx => {
      return trx.insert({id: 1, value: 'One'}).into('users');
    });
    console.log(transactionResult);
  } catch (e) {
    console.log(`Transaction error: ${e}`);
    const user = await client<User>('users').where('id', 1).first();
    console.log(user);
  }
}

async function testReadOnlyTransaction(client: Knex) {
  try {
    const trx = await client.transaction({ readOnly: true });
    const user1 = await trx<User>('users').where('id', 1).first();
    console.log(user1);
    const user2 = await trx<User>('users').where('id', 1).first();
    console.log(user2);
    // Read-only transactions must also be committed or rolled back.
    await trx.commit();
  } catch (e) {
    console.error(`Transaction error: ${e}`);
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
.command(
    'testSelectAllTypes <host> <port> <database>',
    'Selects all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectAllTypes)
)
.command(
    'testInsertAllTypes <host> <port> <database>',
    'Inserts all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypes)
)
.command(
    'testReadWriteTransaction <host> <port> <database>',
    'Runs a read/write transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadWriteTransaction)
)
.command(
    'testReadWriteTransactionError <host> <port> <database>',
    'Runs a read/write transaction that fails',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadWriteTransactionError)
)
.command(
    'testReadOnlyTransaction <host> <port> <database>',
    'Runs a read-only transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadOnlyTransaction)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
