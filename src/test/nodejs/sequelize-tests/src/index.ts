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

const { Sequelize, QueryTypes } = require('sequelize');

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>) {
  const sequelize = new Sequelize(database, null, null, {
    host: host,
    port: port,
    dialect: "postgres",
    dialectOptions: {
      clientMinMessages: "ignore",
      options: "-c spanner.read_only_staleness='MAX_STALENESS 15s'",
    },
    timestamps: false,
    omitNull: false,
    pool: {
      max: 50, min: 10, acquire: 2000, idle: 20000,
    },
    timezone: 'UTC',
    logging: false,
  });
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

require('yargs')
.demand(4)
.command(
    'testSelectUsers <host> <port> <database>',
    'Executes SELECT * FROM users',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectUsers)
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
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
