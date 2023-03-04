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

import {Prisma, PrismaClient} from '@prisma/client'
import {randomUUID} from "crypto";

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>) {
  if (host.charAt(0) == '/') {
    process.env.DATABASE_URL = `postgresql://localhost:${port}/${database}?host=${host}`;
  } else {
    process.env.DATABASE_URL = `postgresql://${host}:${port}/${database}`;
  }
  const prisma = new PrismaClient();
  runTestWithClient(prisma, test)
    .then(async () => {
      await prisma.$disconnect();
    })
    .catch(async (e) => {
      console.error(e);
      await prisma.$disconnect();
      process.exit(1);
    });
}

async function runTestWithClient(client: PrismaClient, test: (client) => Promise<void>) {
  await test(client);
}

async function testSelect1(client: PrismaClient) {
  try {
    const result = await client.$queryRaw`SELECT 1`;
    console.log(result)
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testFindAllUsers(client: PrismaClient) {
  const allUsers = await client.user.findMany({take: 10});
  console.log(allUsers);
}

async function testCreateUser(client: PrismaClient) {
  const newUser = await client.user.create({
    data: {
      id: '2373a81d-772c-4221-adf0-06965bc02c2c',
      name: 'Alice',
      email: 'alice@prisma.io',
    },
  });
  console.log(newUser);
}

async function testCreateAllTypes(client: PrismaClient) {
  const row = await client.allTypes.create({
    data: {
      col_bigint: 1,
      col_bool: true,
      col_bytea: Buffer.from("test", "utf-8"),
      col_float8: 3.14,
      col_int: 100,
      col_numeric: new Prisma.Decimal(6.626),
      col_timestamptz: "2022-02-16T13:18:02.123456Z",
      col_date: new Date("2022-03-29"),
      col_varchar: "test",
      col_jsonb: {"key": "value"},
    },
  });
  console.log(row);
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
    'testFindAllUsers <host> <port> <database>',
    'Executes FindAllUsers',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testFindAllUsers)
)
.command(
    'testCreateUser <host> <port> <database>',
    'Creates a test user',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateUser)
)
.command(
    'testCreateAllTypes <host> <port> <database>',
    'Creates a test row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateAllTypes)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
