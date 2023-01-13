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

import { PrismaClient } from '@prisma/client'

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

require('yargs')
.demand(4)
.command(
    'testSelect1 <host> <port> <database>',
    'Executes SELECT 1',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelect1)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
