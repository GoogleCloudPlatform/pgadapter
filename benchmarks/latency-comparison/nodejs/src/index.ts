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

import {runBenchmark as runPostgresBenchmark} from "./postgresql_benchmark_runner";
import {runBenchmark as runSpannerBenchmark} from "./spanner_benchmark_runner";
import {configDotenv} from "dotenv";
import yargs from "yargs";
import * as os from "os";

export interface Config {
  database: string,
  sql: string,
  readWrite: boolean,
  numOperations: number,
  numClients: number,
}

async function main() {
  configDotenv();
  const project = process.env['GOOGLE_CLOUD_PROJECT'] || 'my-project';
  const instance = process.env['SPANNER_INSTANCE'] || 'my-instance';
  const database = process.env['SPANNER_DATABASE'] || 'my-database';

  const args = await yargs(process.argv).options({
    d: { type: 'string', alias: 'database', default: `projects/${project}/instances/${instance}/databases/${database}`, description: 'The database to use for benchmarking.' },
    c: { type: 'number', alias: 'clients', default: 16, description: 'The number of clients that will be executing queries in parallel.' },
    o: { type: 'number', alias: 'operations', default: 1000, description: 'The number of operations that that each client will run. The total number of operations is clients*operations.' },
    t: { type: 'string', alias: 'transaction', default: 'READ_ONLY', choices: ['READ_ONLY', 'READ_WRITE'], description: 'The transaction type to execute. Must be either READ_ONLY or READ_WRITE.'},
    w: { type: 'number', alias: 'wait', default: 0, description: 'The wait time in milliseconds between each query that is executed by each client. Defaults to 0. Set this to for example 1000 to have each client execute 1 query per second.' },
    e: { type: 'boolean', alias: 'embedded', default: false, description: 'Starts an embedded PGAdapter container along with the benchmark. Setting this option will ignore any host or port settings for PGAdapter.\nNOTE: Running PGAdapter in a Docker container while the application runs on the host machine will add significant latency, as all communication between the application and PGAdapter will have to cross the Docker network bridge. You should only use this option for testing purposes, and not for actual performance benchmarking.' },
    h: { type: 'string', alias: 'host', default: 'localhost', description: 'The host name where PGAdapter is running. Only used if embedded=false.' },
    p: { type: 'number', alias: 'port', default: 5432, description: 'The port number where PGAdapter is running. Only used if embedded=false.' },
    u: { type: 'boolean', alias: 'uds', default: false, description: 'Run a benchmark using Unix Domain Socket in addition to TCP.' },
    dir: { type: 'string', alias: 'udsdir', default: '/tmp', description: 'The directory where PGAdapter listens for Unix Domain Socket connections. Only used if embedded=false.' },
    up: { type: 'number', alias: 'udsport', default: 5432, description: 'The port number where PGAdapter is listening for Unix Domain Sockets. Only used if embedded=false.' },
    warm: { type: 'number', alias: 'warmup', default: 60*1000/5, description: 'The number of warmup iterations to run on PGAdapter before executing the actual benchmark.' },
  }).parse();

  const querySql = "select col_varchar from latency_test where col_bigint=$1"
  const updateSql = "update latency_test set col_varchar=$1 where col_bigint=$2"
  
  // Run a warmup benchmark before collecting results.
  const warmupConfig: Config = {
    sql: querySql,
    readWrite: false,
    numClients: os.cpus().length,
    numOperations: args.warm,
    database: args.d,
  };
  console.log(`Running warmup on database ${warmupConfig.database}`);
  await runPostgresBenchmark(warmupConfig, args.h, args.p);
  console.log('\n');

  const config: Config = {
    sql: querySql,
    readWrite: false,
    numClients: args.c,
    numOperations: args.o,
    database: args.d,
  };
  // Run PGAdapter benchmark.
  console.log(`Running benchmark using PGAdapter on ${args.h}:${args.p} and database ${config.database}`);
  const pgadapterResults = await runPostgresBenchmark(config, args.h, args.p);
  const mergedPgadapterResults = pgadapterResults.flat(1);
  printResults('PGAdapter', mergedPgadapterResults);

  // Run Spanner client library benchmark.
  console.log(`Running benchmark using the Spanner client library on database ${config.database}`);
  const spannerResults = await runSpannerBenchmark(config, args.h, args.p);
  const mergedSpannerResults = spannerResults.flat(1);
  printResults('Spanner Client Library', mergedSpannerResults);
}

function printResults(name: string, results: number[]) {
  const sortedResults = results.sort();
  const sum = sortedResults.reduce((a, b) => a + b, 0);
  const avg = (sum / sortedResults.length) || 0;
  console.log('\n\n');
  console.log('--------------------------------------------------------------');
  console.log(name);
  console.log(`Avg: ${avg}`);
  console.log(`P50: ${sortedResults[Math.floor((sortedResults.length * 50) / 100)]}`);
  console.log(`P95: ${sortedResults[Math.floor((sortedResults.length * 95) / 100)]}`);
  console.log(`P99: ${sortedResults[Math.floor((sortedResults.length * 99) / 100)]}`);
  console.log('\n\n');
}

(async () => {
  await main();
})().catch(e => {
  console.error(e);
});
