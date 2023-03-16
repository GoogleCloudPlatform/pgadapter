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

import {Prisma, PrismaClient, User} from '@prisma/client'

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>, options?: string) {
  if (host.charAt(0) == '/') {
    process.env.DATABASE_URL = `postgresql://localhost:${port}/${database}?host=${host}`;
    if (options) {
      process.env.DATABASE_URL += `&${options}`;
    }
  } else {
    process.env.DATABASE_URL = `postgresql://${host}:${port}/${database}`;
    if (options) {
      process.env.DATABASE_URL += `?${options}`;
    }
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

async function testFindUniqueUser(client: PrismaClient) {
  const user = await client.user.findUnique({where: {id: "1"}})
  console.log(user);
}

async function testFindTwoUniqueUsers(client: PrismaClient) {
  const user1 = await client.user.findUnique({where: {id: "1"}})
  const user2 = await client.user.findUnique({where: {id: "2"}})
  console.log(user1);
  console.log(user2);
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

async function testCreateMultipleUsersWithoutTransaction(client: PrismaClient) {
  await client.user.create({
    data: {
      id: '2373a81d-772c-4221-adf0-06965bc02c2c',
      name: 'Alice',
      email: 'alice@prisma.io',
    },
  });
  await client.user.create({
    data: {
      id: 'e673150f-17ff-451a-8bc7-641fe77f1226',
      name: 'Peter',
      email: 'peter@prisma.io',
    },
  });
  console.log("Created two users");
}

async function testCreateMultipleUsersInTransaction(client: PrismaClient) {
  await client.$transaction(async tx => {
    await tx.user.create({
      data: {
        id: '2373a81d-772c-4221-adf0-06965bc02c2c',
        name: 'Alice',
        email: 'alice@prisma.io',
      },
    });
    await tx.user.create({
      data: {
        id: 'e673150f-17ff-451a-8bc7-641fe77f1226',
        name: 'Peter',
        email: 'peter@prisma.io',
      },
    });
  });
  console.log("Created two users");
}

async function testTransactionIsolationLevel(client: PrismaClient) {
  try {
    await client.$transaction(async tx => {
      await tx.user.create({
        data: {
          id: '2373a81d-772c-4221-adf0-06965bc02c2c',
          name: 'Alice',
          email: 'alice@prisma.io',
        },
      });
      await tx.user.create({
        data: {
          id: 'e673150f-17ff-451a-8bc7-641fe77f1226',
          name: 'Peter',
          email: 'peter@prisma.io',
        },
      });
    }, {
      isolationLevel: Prisma.TransactionIsolationLevel.ReadCommitted,
      maxWait: 5000, // default: 2000
      timeout: 10000, // default: 5000
    });
    console.log("Created two users");
  } catch (e) {
    console.log(`Transaction failed: ${e}`);
  }
}

async function testUnhandledErrorInTransaction(client: PrismaClient) {
  try {
    await client.$transaction(async tx => {
      await tx.user.create({
        data: {
          id: '1',
          name: 'Alice',
          email: 'alice@prisma.io',
        },
      });
      await tx.user.create({
        data: {
          id: '1',
          name: 'Peter',
          email: 'peter@prisma.io',
        },
      });
    });
    console.log("Created two users");
  } catch (e) {
    console.log(`Transaction failed: ${e}`);
  }
}

async function testHandledErrorInTransaction(client: PrismaClient) {
  try {
    await client.$transaction(async tx => {
      try {
        await tx.user.create({
          data: {
            id: '1',
            name: 'Alice',
            email: 'alice@prisma.io',
          },
        });
        console.log("Created user with id 1");
      } catch (e) {
        console.log(`Insert statement failed: ${e}`);
        await tx.user.create({
          data: {
            id: '2',
            name: 'Alice',
            email: 'alice@prisma.io',
          },
        });
        console.log("Created user with id 2");
      }
    });
    console.log("Created one user");
  } catch (e) {
    console.log(`Transaction failed: ${e}`);
  }
}

async function testNestedWrite(client: PrismaClient) {
  // Create a new user with two posts in a
  // single transaction
  const newUser: User = await client.user.create({
    data: {
      id: "1",
      email: 'alice@prisma.io',
      posts: {
        create: [
          { id: "1", title: 'Join the Prisma Slack on https://slack.prisma.io' },
          { id: "2", title: 'Follow @prisma on Twitter' },
        ],
      },
    },
  });
  console.log(newUser);
}

async function testReadOnlyTransaction(client: PrismaClient) {
  await client.$transaction(async tx => {
    await tx.$executeRaw`set transaction read only`;
    const user1 = await tx.user.findUnique({where: {id: "1"}})
    const user2 = await tx.user.findUnique({where: {id: "2"}})
    console.log(user1);
    console.log(user2);
  });
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

async function testUpdateAllTypes(client: PrismaClient) {
  const row = await client.allTypes.update({
    data: {
      col_bool: false,
      col_bytea: Buffer.from("updated", "utf-8"),
      col_float8: 6.626,
      col_int: -100,
      col_numeric: new Prisma.Decimal(3.14),
      col_timestamptz: "2023-03-13T06:40:02.123456+01:00",
      col_date: new Date("2023-03-13"),
      col_varchar: "updated",
      col_jsonb: {"key": "updated"},
    },
    where: {
      col_bigint: 1,
    }
  });
  console.log(row);
}

async function testUpsertAllTypes(client: PrismaClient) {
  const data = {
    col_bool: false,
    col_bytea: Buffer.from("updated", "utf-8"),
    col_float8: 6.626,
    col_int: -100,
    col_numeric: new Prisma.Decimal(3.14),
    col_timestamptz: "2023-03-13T06:40:02.123456+01:00",
    col_date: new Date("2023-03-13"),
    col_varchar: "updated",
    col_jsonb: {"key": "updated"},
  };
  const row = await client.allTypes.upsert({
    create: {col_bigint: 1, ...data},
    update: data,
    where: {
      col_bigint: 1,
    }
  });
  console.log(row);
}

async function testDeleteAllTypes(client: PrismaClient) {
  const row = await client.allTypes.delete({
    where: {col_bigint: 1},
  })
  console.log(row);
}

async function testCreateManyAllTypes(client: PrismaClient) {
  const rows = await client.allTypes.createMany({
    data: [{
      col_bigint: 1,
      col_bool: true,
      col_bytea: Buffer.from("test1", "utf-8"),
      col_float8: 3.14,
      col_int: 100,
      col_numeric: new Prisma.Decimal(6.626),
      col_timestamptz: "2022-02-16T13:18:02.123456+01:00",
      col_date: new Date("2022-03-29"),
      col_varchar: "test1",
      col_jsonb: {"key": "value1"},
    },{
      col_bigint: 2,
      col_bool: false,
      col_bytea: Buffer.from("test2", "utf-8"),
      col_float8: -3.14,
      col_int: -100,
      col_numeric: new Prisma.Decimal(-6.626),
      col_timestamptz: "2022-02-16T13:18:02.123456-01:00",
      col_date: new Date("2022-03-30"),
      col_varchar: "test2",
      col_jsonb: {"key": "value2"},
    }],
  });
  console.log(rows);
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
    'testFindUniqueUser <host> <port> <database>',
    'Executes FindUniqueUser',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testFindUniqueUser)
)
.command(
    'testFindTwoUniqueUsers <host> <port> <database>',
    'Executes FindTwoUniqueUsers',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testFindTwoUniqueUsers)
)
.command(
    'testFindTwoUniqueUsersUsingStaleRead <host> <port> <database>',
    'Executes FindTwoUniqueUsers using a client that issues stale reads',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testFindTwoUniqueUsers, "options=-c spanner.read_only_staleness='MAX_STALENESS 10s'")
)
.command(
    'testCreateUser <host> <port> <database>',
    'Creates a test user',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateUser)
)
.command(
    'testCreateMultipleUsersWithoutTransaction <host> <port> <database>',
    'Creates two test users without a transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateMultipleUsersWithoutTransaction)
)
.command(
    'testCreateMultipleUsersInTransaction <host> <port> <database>',
    'Creates two test users in a transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateMultipleUsersInTransaction)
)
.command(
    'testUnhandledErrorInTransaction <host> <port> <database>',
    'Tries to create two test users with the same id in a transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testUnhandledErrorInTransaction)
)
.command(
    'testHandledErrorInTransaction <host> <port> <database>',
    'Tries to continue to use the transaction after an error',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testHandledErrorInTransaction)
)
.command(
    'testTransactionIsolationLevel <host> <port> <database>',
    'Uses a transaction with read-committed isolation level',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testTransactionIsolationLevel)
)
.command(
    'testNestedWrite <host> <port> <database>',
    'Creates a test user',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testNestedWrite)
)
.command(
    'testReadOnlyTransaction <host> <port> <database>',
    'Uses a read-only transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadOnlyTransaction)
)
.command(
    'testCreateAllTypes <host> <port> <database>',
    'Creates a test row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateAllTypes)
)
.command(
    'testUpdateAllTypes <host> <port> <database>',
    'Updates a test row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testUpdateAllTypes)
)
.command(
    'testUpsertAllTypes <host> <port> <database>',
    'Upserts a test row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testUpsertAllTypes)
)
.command(
    'testDeleteAllTypes <host> <port> <database>',
    'Deletes a test row with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testDeleteAllTypes)
)
.command(
    'testCreateManyAllTypes <host> <port> <database>',
    'Creates multiple test rows with all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCreateManyAllTypes)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
