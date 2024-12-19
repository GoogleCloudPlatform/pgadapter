// Copyright 2024 Google LLC
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

import {Config, generate_random_string} from "./index";
import {Database, Spanner} from "@google-cloud/spanner";
import {Json} from "@google-cloud/spanner/build/src/codec";
import {randomInt} from "crypto";

let totalOperations: number;
let progress: number;
let numNull: number;
let numNotNull: number;

export async function runBenchmark(config: Config): Promise<number[][]> {
  progress = 0;
  numNull = 0;
  numNotNull = 0;
  totalOperations = config.numClients * config.numOperations;
  
  const progressPrinter = setInterval(printProgress, 1000);
  const databaseNameParts = config.database.split('/');
  const projectId = databaseNameParts[1];
  const instanceId = databaseNameParts[3];
  const databaseId = databaseNameParts[5];

  const spanner = new Spanner({projectId: projectId});
  const instance = spanner.instance(instanceId);
  const database = instance.database(databaseId);
  
  const promises: Promise<number[]>[] = [];
  for (let i=0; i<config.numClients; i++) {
    promises.push(run(database, config));
  }
  const results = await Promise.all(promises);

  // Close the connection.
  spanner.close();
  clearInterval(progressPrinter);
  printProgress();
  return results;
}

async function run(database: Database, config: Config): Promise<number[]> {
  const results: number[] = [];
  for (let i= 0; i < config.numOperations; i++) {
    const start = performance.now();
    const id: number = getRandomInt(100000);
    if (config.readWrite) {
      await executeUpdate(database, config, id);
    } else {
      await executeQuery(database, config, id);
    }
    progress++;
    const elapsed = performance.now() - start;
    results.push(elapsed);
    if (config.wait > 0) {
      const t = randomInt(0, 2 * config.wait);
      await new Promise(f => setTimeout(f, t));
    }
  }
  return results;
}

async function executeQuery(database: Database, config: Config, id: number) {
  const [rows] = await database.run({
    sql: config.sql,
    params: {p1: `${id}`},
    types: {p1: {type: 'int64'}},
    json: true,
  });
  for (const row of rows) {
    if ((row as Json).col_varchar) {
      numNotNull++;
    } else {
      numNull++;
    }
  }
}

async function executeUpdate(database: Database, config: Config, id: number) {
  await database.runTransactionAsync(transaction => {
    return transaction.runUpdate({
      sql: config.sql,
      params: {p1: generate_random_string(64), p2: `${id}`},
      types: {p1: {type: 'string'}, p2: {type: 'int64'}},
    });
  });
}

function printProgress() {
  if (process.stdout.clearLine) {
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
  }
  process.stdout.write(`${progress}/${totalOperations}`);
}

function getRandomInt(max: number) {
  return Math.floor(Math.random() * max);
}
