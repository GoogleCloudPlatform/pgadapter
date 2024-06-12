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

import {DataTypes, literal, Op} from "sequelize";

const { Sequelize, QueryTypes } = require('sequelize');

let User: any;
let AllTypes: any;

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>, staleRead: boolean = false) {
  let options = {
    host: host,
    port: port,
    dialect: "postgres",
    timestamps: false,
    omitNull: false,
    pool: {
      max: 50, min: 10, acquire: 2000, idle: 20000,
    },
    timezone: 'UTC',
    logging: false,
  };
  if (staleRead) {
    Object.assign(options, {dialectOptions: {
        clientMinMessages: "ignore",
        options: "-c spanner.read_only_staleness='MAX_STALENESS 15s'",
      }});
  }
  const sequelize = new Sequelize(database, null, null, options);
  AllTypes = sequelize.define(
      'all_types',
      {
        col_bigint: {type: DataTypes.BIGINT, primaryKey: true},
        col_bool: {type: DataTypes.BOOLEAN},
        col_bytea: {type: DataTypes.BLOB},
        col_float4: {type: DataTypes.FLOAT},
        col_float8: {type: DataTypes.DOUBLE},
        col_int: {type: DataTypes.INTEGER},
        col_numeric: {type: DataTypes.DECIMAL},
        col_timestamptz: {type: DataTypes.DATE},
        col_date: {type: DataTypes.DATEONLY},
        col_varchar: {type: DataTypes.STRING},
        col_jsonb: {type: DataTypes.JSONB},
      },
      {
        createdAt: false,
        updatedAt: false,
      },
  );
  User = sequelize.define(
      'users',
      {
        name: {type: DataTypes.STRING},
      }
  );

  runTestWithClient(sequelize, test);
}

function runTestWithClient(client, test: (client) => Promise<void>) {
  client.authenticate()
    .then(() => {
      test(client).then(() => client.close());
    })
    .catch((error) => {
      console.error(error);
      client.close();
    });
}

async function testSelectUsers(client) {
  try {
    const rows = await client.query("SELECT * FROM users", { type: QueryTypes.SELECT });
    if (rows) {
      console.log(`Users: ${Object.values(rows[0])}`);
    } else {
      console.error('Could not select users');
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testSelectUsersInTransaction(client) {
  const t = await client.transaction();
  try {
    const rows = await client.query("SELECT * FROM users", { type: QueryTypes.SELECT, transaction: t });
    console.log(`Users: ${Object.values(rows[0])}`);
    await t.commit();
  } catch (e) {
    console.error(`Query error: ${e}`);
    await t.rollback();
  }
}

async function testErrorInTransaction(client) {
  try {
    await client.transaction(async tx => {
      const rows = await client.query("SELECT * FROM users", { type: QueryTypes.SELECT, transaction: tx });
      console.log(`Users: ${Object.values(rows[0])}`);
      await client.query("SELECT * FROM non_existing_table", {
        type: QueryTypes.SELECT,
        transaction: tx
      });
    });
  } catch (e) {
    console.log(`Transaction error: ${e}`);
  }
}

async function testSelectAllTypes(client) {
  try {
    const id = 1;
    const rows = await AllTypes.findAll({where: literal('col_bigint = $id'), bind: {id}});
    console.log(JSON.stringify(rows, null, 2));
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testInsertAllTypes(client) {
  try {
    for (let i=0; i<2; i++) {
      const row = await AllTypes.create({
        col_bigint: i+1,
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
      });
      console.log(`Inserted row with id ${row.col_bigint}`);
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testUnmanagedReadWriteTransaction(client) {
  try {
    const transaction = await client.transaction();
    const id = 1;
    const user = await User.findOne({where: { id: {[Op.eq]: literal('$id')} }, bind: {id}, transaction: transaction});
    console.log(JSON.stringify(user, null, 2));
    const newUser = await User.create({name: 'Test'}, {transaction});
    console.log(JSON.stringify(newUser, null, 2));
    await transaction.commit();
  } catch (e) {
    console.error(`Transaction error: ${e}`);
  }
}

async function testManagedReadWriteTransaction(client) {
  try {
    await client.transaction(async (transaction: any) => {
      const id = 1;
      const user = await User.findOne({where: { id: {[Op.eq]: literal('$id')} }, bind: {id}, transaction: transaction});
      console.log(JSON.stringify(user, null, 2));
      const newUser = await User.create({name: 'Test'}, {transaction});
      console.log(JSON.stringify(newUser, null, 2));
      return newUser;
    });
  } catch (e) {
    console.error(`Transaction error: ${e}`);
  }
}

async function testContinueAfterFailedTransaction(client) {
  try {
    await client.transaction(async (transaction: any) => {
      const id = 1;
      await User.findOne({where: { id: {[Op.eq]: literal('$id')} }, bind: {id}, transaction: transaction});
      return User.create({name: 'Test'}, {transaction});
    });
  } catch (e) {
    console.log(`Transaction error: ${e}`);
    const id = 1;
    const user = await User.findOne({where: { id: {[Op.eq]: literal('$id')} }, bind: {id}});
    console.log(JSON.stringify(user, null, 2));
  }
}

require('yargs')
.demand(4)
.command(
    'testSelectUsers <host> <port> <database>',
    'Executes SELECT * FROM users',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectUsers, true)
)
.command(
    'testSelectUsersInTransaction <host> <port> <database>',
    'Executes SELECT * FROM users in a transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectUsersInTransaction)
)
.command(
    'testErrorInTransaction <host> <port> <database>',
    'Executes a statement in a transaction that fails',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testErrorInTransaction)
)
.command(
    'testSelectAllTypes <host> <port> <database>',
    'Queries a table with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectAllTypes)
)
.command(
    'testInsertAllTypes <host> <port> <database>',
    'Inserts a row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypes)
)
.command(
    'testUnmanagedReadWriteTransaction <host> <port> <database>',
    'Executes a simple read/write transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testUnmanagedReadWriteTransaction)
)
.command(
    'testManagedReadWriteTransaction <host> <port> <database>',
    'Executes a managed read/write transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testManagedReadWriteTransaction)
)
.command(
    'testContinueAfterFailedTransaction <host> <port> <database>',
    'Executes a managed read/write transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testContinueAfterFailedTransaction)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
