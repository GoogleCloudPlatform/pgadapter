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

import {createDataModel, prisma} from "./index";

function runTest(host: string, port: number, database: string, test: () => Promise<void>) {
  runTestWithClient(test)
    .then(async () => {
      await prisma.$disconnect();
    })
    .catch(async (e) => {
      console.error(e);
      await prisma.$disconnect();
      process.exit(1);
    });
}

async function runTestWithClient(test: () => Promise<void>) {
  await test();
}

require('yargs')
.demand(4)
.command(
    'testCreateDataModel <host> <port> <database>',
    'Creates the sample data model',
    {},
    opts => runTest(opts.host, opts.port, opts.database, createDataModel)
)


