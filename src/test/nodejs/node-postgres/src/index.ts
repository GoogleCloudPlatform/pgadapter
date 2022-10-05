// Copyright 2022 Google LLC
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

import "reflect-metadata"
const { Client } = require('pg')

function runTest(host: string, port: number, database: string, test: (client) => Promise<void>) {
  const client = new Client({
    host,
    port,
    database,
  })
  runTestWithClient(client, test);
}

function runTestWithClient(client, test: (client) => Promise<void>) {
  client.connect()
  .then(() => {
    test(client).then(() => client.end());
  })
  .catch((error) => console.log(error));
}

async function testSelect1(client) {
  try {
    const {rows} = await client.query('SELECT 1');
    if (rows) {
      console.log(`SELECT 1 returned: ${Object.values(rows[0])[0]}`);
    } else {
      console.error('Could not select 1');
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testInsert(client) {
  try {
    await client.query('BEGIN');
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res.rowCount} row(s)`);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertTwice(client) {
  try {
    await client.query('BEGIN');
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res1 = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res1.rowCount} row(s)`);
    const res2 = await client.query(queryText, ['bar']);
    console.log(`Inserted ${res2.rowCount} row(s)`);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAutoCommit(client) {
  try {
    const queryText = 'INSERT INTO users(name) VALUES($1)';
    const res = await client.query(queryText, ['foo']);
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypes(client) {
  try {
    const queryText = 'INSERT INTO AllTypes ' +
        '(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) ' +
        'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';
    const res = await client.query(queryText, [
        1, true, Buffer.from('some random string', 'utf-8'),
        3.14, 100, 234.54235, new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
        '2022-07-22', 'some-random-string', { my_key: "my-value" }]);
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypesNull(client) {
  try {
    const queryText = 'INSERT INTO AllTypes ' +
        '(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) ' +
        'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';
    const res = await client.query(queryText, [
        null, null, null, null, null, null, null, null, null, null]);
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypesPreparedStatement(client) {
  try {
    const query = {
      name: 'insert-all-types',
      text: 'INSERT INTO AllTypes ' +
          '(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) ' +
          'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
      values: [
        1, true, Buffer.from('some random string', 'utf-8'),
        3.14, 100, 234.54235, new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
        '2022-07-22', 'some-random-string', { my_key: "my-value" }],
    }
    // Execute the statement twice.
    const res1 = await client.query(query);
    console.log(`Inserted ${res1.rowCount} row(s)`);
    const res2 = await client.query({
      name: 'insert-all-types',
      values: [null, null, null, null, null, null, null, null, null, null],
    });
    console.log(`Inserted ${res2.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testSelectAllTypes(client) {
  const pg = require('pg')
  // Make sure that DATE data types are shown as string values, otherwise node-postgres will convert
  // it to a Javascript date object using the local timezone.
  pg.types.setTypeParser(1082, function(stringValue) {
    return stringValue;
  });
  try {
    const queryText = 'SELECT col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb ' +
        'FROM AllTypes';
    const result = await client.query(queryText);
    console.log(`Selected ${JSON.stringify(result.rows[0])}`);
  } catch (e) {
    console.error(`Select error: ${e}`);
  }
}

async function testErrorInReadWriteTransaction(client) {
  const queryText = 'INSERT INTO users(name) VALUES($1)';
  try {
    await client.query('BEGIN');
    await client.query('SELECT 1');
    await client.query(queryText, ['foo']);
    console.error(`Insert unexpectedly succeeded.`);
  } catch (e) {
    // This is expected.
    console.log(`Insert error: ${e}`);
    // It should not be possible to use the connection until a ROLLBACK is executed.
    try {
      await client.query(queryText);
      console.error('Second insert unexpectedly succeeded.')
    } catch (innerError) {
      console.log(`Second insert failed with error: ${innerError}`);
      await client.query('ROLLBACK');
      // The connection should now be usable.
      const {rows} = await client.query('SELECT 1');
      console.log(`SELECT 1 returned: ${Object.values(rows[0])[0]}`);
    }
  }
}

async function testReadOnlyTransaction(client) {
  try {
    await client.query('begin read only');
    await client.query('SELECT 1');
    await client.query('SELECT 2');
    await client.query('commit');
    console.log('executed read-only transaction');
  } catch (e) {
    console.error(`Select error: ${e}`);
  }
}

async function testReadOnlyTransactionWithError(client) {
  try {
    await client.query('begin read only');
    try {
      await client.query('SELECT * FROM foo');
      console.error('select statement unexpectedly succeeded');
    } catch (e) {
      // This is expected.
    }
    try {
      // This should fail as the transaction is aborted.
      await client.query('SELECT 1');
      console.error('SELECT 1 unexpectedly succeeded');
    } catch (e) {
      console.log(e.message);
    }
    await client.query('rollback');
    const res = await client.query('SELECT 2');
    console.log(res.rows);
  } catch (e) {
    console.error(`Read-only transaction error: ${e}`);
  }
}

async function testCopyTo(client) {
  try {
    const copyTo = require('pg-copy-streams').to;
    const stream = client.query(copyTo('COPY AllTypes TO STDOUT'));
    stream.pipe(process.stdout);
    await new Promise<void>(function(resolve, reject) {
      stream.on('end', () => resolve());
      stream.on('error', reject);
    });
  } catch (e) {
    console.error(`COPY error: ${e}`);
  }
}

async function testCopyFrom(client) {
  try {
    const copyFrom = require('pg-copy-streams').from;
    const fs = require('fs');
    const stream = client.query(copyFrom('COPY all_types FROM STDIN'));
    const fileStream = fs.createReadStream('../../resources/all_types_data_small.txt');
    fileStream.pipe(stream);
    await new Promise<void>(function(resolve, reject) {
      stream.on('finish', () => resolve());
      stream.on('error', reject);
      fileStream.on('error', reject);
    });
    console.log('Finished copy operation');
  } catch (e) {
    console.error(`COPY error: ${e}`);
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
.command(
    'testInsert <host> <port> <database>',
    'Inserts a single row',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsert)
)
.command(
    'testInsertTwice <host> <port> <database>',
    'Executes the same parameterized insert statement twice',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertTwice)
)
.command(
    'testInsertAutoCommit <host> <port> <database>',
    'Inserts a single row using auto commit',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAutoCommit)
)
.command(
    'testInsertAllTypes <host> <port> <database>',
    'Inserts a row using all supported types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypes)
)
.command(
    'testInsertAllTypesNull <host> <port> <database>',
    'Inserts a row using all supported types with only null values',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypesNull)
)
.command(
    'testSelectAllTypes <host> <port> <database>',
    'Selects a row with columns containing all supported types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectAllTypes)
)
.command(
    'testInsertAllTypesPreparedStatement <host> <port> <database>',
    'Inserts a row using all supported types with a prepared statement',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypesPreparedStatement)
)
.command(
    'testErrorInReadWriteTransaction <host> <port> <database>',
    'Verifies that an error in a transactions renders the transaction unusable',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testErrorInReadWriteTransaction)
)
.command(
    'testReadOnlyTransaction <host> <port> <database>',
    'Tests a read-only transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadOnlyTransaction)
)
.command(
    'testReadOnlyTransactionWithError <host> <port> <database>',
    'Tests a read-only transaction with a statement that returns an error',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadOnlyTransactionWithError)
)
.command(
    'testCopyTo <host> <port> <database>',
    'Tests COPY to stdout',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCopyTo)
)
.command(
    'testCopyFrom <host> <port> <database>',
    'Tests COPY from stdin',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testCopyFrom)
)
.wrap(120)
.recommendCommands()
.strict()
.help().argv;
