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

import {PrismaClient} from '@prisma/client'

const prisma = new PrismaClient();
const staleReadClient = new PrismaClient({
  datasources: {
    db: {
      url: `${process.env.DATABASE_URL}?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'`,
    },
  },
})

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
