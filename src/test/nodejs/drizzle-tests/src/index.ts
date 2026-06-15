import { Client } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql, eq } from 'drizzle-orm';
import * as schema from './schema';
import { users, allTypes, posts } from './schema';

async function runTest(host: string, port: number, database: string, test: (db: any) => Promise<void>) {
  const client = new Client({
    host,
    port,
    database,
  });
  await client.connect();
  const db = drizzle(client, { schema });
  try {
    await test(db);
  } catch (error) {
    console.error(error);
  } finally {
    await client.end();
  }
}

async function testSelect1(db: any) {
  try {
    const result = await db.execute(sql`SELECT 1`);
    if (result && result.rows && result.rows.length > 0) {
      console.log(`SELECT 1 returned: ${Object.values(result.rows[0])[0]}`);
    } else {
      console.error('Could not select 1');
    }
  } catch (e) {
    console.error(`Query error: ${e}`);
  }
}

async function testInsert(db: any) {
  try {
    await db.transaction(async (tx) => {
      const res = await tx.insert(users).values({ name: 'foo' });
      console.log(`Inserted ${res.rowCount} row(s)`);
    });
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertTwice(db: any) {
  try {
    await db.transaction(async (tx) => {
      const res1 = await tx.insert(users).values({ name: 'foo' });
      console.log(`Inserted ${res1.rowCount} row(s)`);
      const res2 = await tx.insert(users).values({ name: 'bar' });
      console.log(`Inserted ${res2.rowCount} row(s)`);
    });
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAutoCommit(db: any) {
  try {
    const res = await db.insert(users).values({ name: 'foo' });
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypes(db: any) {
  try {
    const res = await db.insert(allTypes).values({
      col_bigint: BigInt(1),
      col_bool: true,
      col_bytea: Buffer.from('some random string', 'utf-8'),
      col_float4: 3.14,
      col_float8: 3.14,
      col_int: BigInt(100),
      col_numeric: '234.54235',
      col_timestamptz: new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
      col_date: '2022-07-22',
      col_varchar: 'some-random-string',
      col_jsonb: { my_key: "my-value" }
    });
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testInsertAllTypesAllNull(db: any) {
  try {
    const res = await db.insert(allTypes).values({
      col_bigint: BigInt(1),
      col_bool: null,
      col_bytea: null,
      col_float4: null,
      col_float8: null,
      col_int: null,
      col_numeric: null,
      col_timestamptz: null,
      col_date: null,
      col_varchar: null,
      col_jsonb: null
    });
    console.log(`Inserted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error(`Insert error: ${e}`);
  }
}

async function testSelectAllTypes(db: any) {
  try {
    const rows = await db.select().from(allTypes);
    if (rows && rows.length > 0) {
      const row = rows[0];
      // Convert BigInts to string for JSON serialization comparison stability
      const serializedRow = JSON.stringify(row, (key, value) =>
        typeof value === 'bigint' ? value.toString() : value
      );
      console.log(`Selected ${serializedRow}`);
    } else {
      console.error('No rows selected');
    }
  } catch (e) {
    console.error(`Select error: ${e}`);
  }
}

async function testErrorInReadWriteTransaction(db: any) {
  try {
    await db.transaction(async (tx) => {
      await tx.execute(sql`SELECT 1`);
      // First insert will succeed or fail depending on Spanner state, let's try inserting 'foo'
      await tx.insert(users).values({ name: 'foo' });
    });
  } catch (e) {
    console.log(`Insert error: ${e}`);
    // Drizzle will auto-rollback. Let's verify that a subsequent statement on connection outside transaction works.
    try {
      const result = await db.execute(sql`SELECT 1`);
      console.log(`SELECT 1 returned: ${Object.values(result.rows[0])[0]}`);
    } catch (innerError) {
      console.error(`Subsequent query failed: ${innerError}`);
    }
  }
}

async function testReadOnlyTransaction(db: any) {
  try {
    // Drizzle transaction supports accessMode option: 'read only' or 'read write'
    // Let's use readOnly mode.
    await db.transaction(async (tx) => {
      await tx.execute(sql`SELECT 1`);
      await tx.execute(sql`SELECT 2`);
    }, { behavior: 'read only' });
    console.log('executed read-only transaction');
  } catch (e) {
    console.error(`Read-only transaction error: ${e}`);
  }
}

async function testUpdate(db: any) {
  try {
    const res = await db.update(allTypes).set({ col_varchar: 'bar' }).where(eq(allTypes.col_bigint, BigInt(1)));
    console.log(`Updated ${res.rowCount} row(s)`);
  } catch (e) {
    console.error("Update error:", e);
  }
}

async function testDelete(db: any) {
  try {
    const res = await db.delete(users).where(eq(users.name, 'bar'));
    console.log(`Deleted ${res.rowCount} row(s)`);
  } catch (e) {
    console.error("Delete error:", e);
  }
}

async function testSelectRelationalQueries(db: any) {
  try {
    const results = await db.query.users.findMany({
      with: {
        posts: true,
      },
    });
    console.log("Relational query returned:", JSON.stringify(results));
  } catch (e) {
    console.error("Relational query error:", e);
  }
}

require('yargs')
  .demand(4)
  .command(
    'testSelectRelationalQueries <host> <port> <database>',
    'Executes relational queries',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectRelationalQueries)
  )
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
    'Inserts twice',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertTwice)
  )
  .command(
    'testInsertAutoCommit <host> <port> <database>',
    'Inserts with auto-commit',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAutoCommit)
  )
  .command(
    'testInsertAllTypes <host> <port> <database>',
    'Inserts all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypes)
  )
  .command(
    'testInsertAllTypesAllNull <host> <port> <database>',
    'Inserts all types null',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testInsertAllTypesAllNull)
  )
  .command(
    'testSelectAllTypes <host> <port> <database>',
    'Selects all types',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testSelectAllTypes)
  )
  .command(
    'testErrorInReadWriteTransaction <host> <port> <database>',
    'Verifies transaction error recovery',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testErrorInReadWriteTransaction)
  )
  .command(
    'testUpdate <host> <port> <database>',
    'Updates a row',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testUpdate)
  )
  .command(
    'testDelete <host> <port> <database>',
    'Deletes a row',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testDelete)
  )
  .command(
    'testReadOnlyTransaction <host> <port> <database>',
    'Tests read-only transaction',
    {},
    opts => runTest(opts.host, opts.port, opts.database, testReadOnlyTransaction)
  )
  .wrap(120)
  .recommendCommands()
  .strict()
  .help().argv;
