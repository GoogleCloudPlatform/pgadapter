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

import createTables from "./create_tables";
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import createConnection from "./create_connection";
import writeDataWithDml from "./write_data_with_dml";
import writeDataWithDmlBatch from "./write_data_with_dml_batch";
import writeDataWithCopy from "./write_data_with_copy";
import queryData from "./query_data";
import queryWithParameter from "./query_data_with_parameter";
import addColumn from "./add_column";
import ddlBatch from "./ddl_batch";
import updateDataWithCopy from "./update_data_with_copy";
import queryDataWithNewColumn from "./query_data_with_new_column";
import writeWithTransactionUsingDml from "./update_data_with_transaction";
import tags from "./tags";
import readOnlyTransaction from "./read_only_transaction";
import dataBoost from "./data_boost";

yargs(hideBin(process.argv))
  .command('createtables <database> [host] [port]', 'Create sample tables', (yargs) => {
    return yargs
        .positional('database', {describe: "Sample database name", type: "string"})
        .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
        .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await createTables(argv.host, argv.port, argv.database);
  })
  .command('createconnection <database> [host] [port]', 'Connect to PGAdapter', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await createConnection(argv.host, argv.port, argv.database);
  })
  .command('writeusingdml <database> [host] [port]', 'Write data using DML', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await writeDataWithDml(argv.host, argv.port, argv.database);
  })
  .command('writeusingdmlbatch <database> [host] [port]', 'Write data using a DML batch', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await writeDataWithDmlBatch(argv.host, argv.port, argv.database);
  })
  .command('write <database> [host] [port]', 'Write data using the COPY command. This is translated to Mutations on Spanner.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await writeDataWithCopy(argv.host, argv.port, argv.database);
  })
  .command('query <database> [host] [port]', 'Run a sample query', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await queryData(argv.host, argv.port, argv.database);
  })
  .command('querywithparameter <database> [host] [port]', 'Run a parameterized query', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await queryWithParameter(argv.host, argv.port, argv.database);
  })
  .command('addmarketingbudget <database> [host] [port]', 'Add a column to a table', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await addColumn(argv.host, argv.port, argv.database);
  })
  .command('ddlbatch <database> [host] [port]', 'Execute a batch of DDL statements', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await ddlBatch(argv.host, argv.port, argv.database);
  })
  .command('update <database> [host] [port]', 'Update data using the COPY command. This is translated to mutations on Spanner.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await updateDataWithCopy(argv.host, argv.port, argv.database);
  })
  .command('querymarketingbudget <database> [host] [port]', 'Query data from the newly added column.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await queryDataWithNewColumn(argv.host, argv.port, argv.database);
  })
  .command('writewithtransactionusingdml <database> [host] [port]', 'Execute a read/write transaction and update data using DML.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await writeWithTransactionUsingDml(argv.host, argv.port, argv.database);
  })
  .command('tags <database> [host] [port]', 'Add tags to transactions and statements.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await tags(argv.host, argv.port, argv.database);
  })
  .command('readonlytransaction <database> [host] [port]', 'Execute a read-only transaction.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await readOnlyTransaction(argv.host, argv.port, argv.database);
  })
  .command('databoost <database> [host] [port]', 'Execute a query using Data Boost.', (yargs) => {
    return yargs
    .positional('database', {describe: "Sample database name", type: "string"})
    .positional('host', {optional: true, describe: "PGAdapter host name", type: "string", default: "localhost"})
    .positional('port', {optional: true, describe: "PGAdapter port number", type: "number", default: 5432});
  }, async (argv) => {
    await dataBoost(argv.host, argv.port, argv.database);
  })
  .demandCommand(1)
  .parse();
