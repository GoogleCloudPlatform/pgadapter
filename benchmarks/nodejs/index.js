const {Spanner} = require('@google-cloud/spanner');
const { Pool } = require('pg');

const projectId = 'spanner-pg-preview-internal';
const instanceId = 'europe-north1';
const databaseId = 'knut-test-db';

function getRandomInt(max) {
    return Math.floor(Math.random() * max);
}

async function test() {

    // Creates a Spanner client
    const spanner = new Spanner({projectId});
    // Creates a PG client pool.
    const pool = new Pool({
        user: 'user',
        host: '/tmp',
        database: 'knut-test-db',
        password: 'password',
        port: 5432,
        max: 400,
    });

    // Gets a reference to a Cloud Spanner instance and database
    const instance = spanner.instance(instanceId);
    const database = instance.database(databaseId);

    // Make sure the session pools have been initialized.
    await database.run('select 1');
    await pool.query('select 1');

    await spannerSelectRowsSequentially(database, 100);
    await spannerSelectMultipleRows(database, 20, 500);
    await spannerSelectAndUpdateRows(database, 20, 5);

    await spannerSelectRowsInParallel(database, 1000);
    await spannerSelectMultipleRowsInParallel(database, 200, 500);
    await spannerSelectAndUpdateRowsInParallel(database, 200, 5);

    await pgSelectRowsSequentially(pool, 100);
    await pgSelectMultipleRows(pool, 20, 500);
    await pgSelectAndUpdateRows(pool, 20, 5);

    await pgSelectRowsInParallel(pool, 1000);
    await pgSelectMultipleRowsInParallel(pool, 200, 500);
    await pgSelectAndUpdateRowsInParallel(pool, 200, 5);

    await database.close();
    await pool.end();
}

async function spannerSelectRowsSequentially(database, numQueries) {
    console.log(`Selecting ${numQueries} rows sequentially`);
    const start = new Date();
    for (let i = 0; i < numQueries; i++) {
        const query = {
            sql: 'SELECT * FROM all_types WHERE col_bigint=$1',
            params: {
                p1: getRandomInt(5000000),
            },
        };

        await database.run(query);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time for selecting ${numQueries} rows sequentially: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numQueries}ms`);
}

async function spannerSelectRowsInParallel(database, numQueries) {
    console.log(`Selecting ${numQueries} rows in parallel`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numQueries; i++) {
        const query = {
            sql: 'SELECT * FROM all_types WHERE col_bigint=$1',
            params: {
                p1: getRandomInt(5000000),
            },
        };

        promises.push(database.run(query));
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    console.log("Waiting for queries to finish");
    const allRows = await Promise.all(promises);
    allRows.forEach(rows => {
        if (rows[0].length < 0 || rows[0].length > 1) {
            console.log(`Unexpected row count: ${rows[0].length}`);
        }
    });
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time for selecting ${numQueries} rows in parallel: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numQueries}ms`);
}

async function spannerSelectMultipleRows(database, numQueries, numRows) {
    console.log(`Selecting ${numQueries} with each ${numRows} rows sequentially`);
    const start = new Date();
    for (let i = 0; i < numQueries; i++) {
        const query = {
            sql: `SELECT * FROM all_types WHERE col_bigint>$1 LIMIT ${numRows}`,
            params: {
                p1: getRandomInt(5000000),
            },
            json: true,
        };
        await database.run(query);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time for executing ${numQueries} with ${numRows} rows each: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numQueries}ms`);
}

async function spannerSelectMultipleRowsInParallel(database, numQueries, numRows) {
    console.log(`Selecting ${numQueries} with each ${numRows} rows in parallel`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numQueries; i++) {
        const query = {
            sql: `SELECT * FROM all_types WHERE col_bigint>$1 LIMIT ${numRows}`,
            params: {
                p1: getRandomInt(5000000),
            },
            json: true,
        };
        promises.push(database.run(query));
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    console.log("Waiting for queries to finish");
    const allRows = await Promise.all(promises);
    allRows.forEach(rows => {
        if (rows[0].length < 0 || rows[0].length > numRows) {
            console.log(`Unexpected row count: ${rows[0].length}`);
        }
    });
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time for executing ${numQueries} with ${numRows} rows each in parallel: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numQueries}ms`);
}

async function spannerSelectAndUpdateRows(database, numTransactions, numRowsPerTx) {
    console.log(`Executing ${numTransactions} with each ${numRowsPerTx} rows per transaction`);
    const start = new Date();
    for (let i = 0; i < numTransactions; i++) {
        await database.runTransactionAsync(selectAndUpdate);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time executing ${numTransactions} with ${numRowsPerTx} rows each: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numTransactions}ms`);
}

async function spannerSelectAndUpdateRowsInParallel(database, numTransactions, numRowsPerTx) {
    console.log(`Executing ${numTransactions} with each ${numRowsPerTx} rows per transaction in parallel`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numTransactions; i++) {
        promises.push(database.runTransactionAsync(selectAndUpdate));
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    console.log("Waiting for transactions to finish");
    await Promise.all(promises);
    const end = new Date();
    const elapsed = end - start;
    console.log(`Execution time executing ${numTransactions} with ${numRowsPerTx} rows each: ${elapsed}ms`);
    console.log(`Avg execution time: ${elapsed/numTransactions}ms`);
}

async function selectAndUpdate(tx) {
    const query = {
        sql: 'SELECT * FROM all_types WHERE col_bigint=$1',
        params: {
            p1: getRandomInt(5000000),
        },
        json: true,
    };
    const [rows] = await tx.run(query);
    if (rows.length === 1) {
        rows[0].col_float8 = Math.random();
        const update = {
            sql: 'UPDATE all_types SET col_float8=$1 WHERE col_bigint=$2',
            params: {
                p1: rows[0].col_float8,
                p2: rows[0].col_bigint,
            },
        }
        const [rowCount] = await tx.runUpdate(update);
        if (rowCount !== 1) {
            console.error(`Unexpected update count: ${rowCount}`);
        }
    }
    await tx.commit();
}

async function pgSelectRowsSequentially(pool, numQueries) {
    console.log(`PG: Selecting ${numQueries} rows sequentially`);
    const start = new Date();
    for (let i = 0; i < numQueries; i++) {
        await pool.query('SELECT * FROM all_types WHERE col_bigint=$1', [getRandomInt(5000000)]);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time for selecting ${numQueries} rows sequentially: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numQueries}ms`);
}

async function pgSelectRowsInParallel(pool, numQueries) {
    console.log(`PG: Selecting ${numQueries} rows in parallel`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numQueries; i++) {
        promises.push(pool.query('SELECT * FROM all_types WHERE col_bigint=$1', [getRandomInt(5000000)]));
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    console.log("Waiting for queries to finish");
    const allRows = await Promise.all(promises);
    allRows.forEach(result => {
        if (result.rows.length < 0 || result.rows.length > 1) {
            console.log(`Unexpected row count: ${result.rows.length}`);
        }
    });
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time for selecting ${numQueries} rows in parallel: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numQueries}ms`);
}

async function pgSelectMultipleRows(pool, numQueries, numRows) {
    console.log(`PG: Selecting ${numQueries} with each ${numRows} rows sequentially`);
    const start = new Date();
    for (let i = 0; i < numQueries; i++) {
        const sql = `SELECT * FROM all_types WHERE col_bigint>$1 LIMIT ${numRows}`;
        await pool.query(sql, [getRandomInt(5000000)]);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time for executing ${numQueries} with ${numRows} rows each: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numQueries}ms`);
}

async function pgSelectMultipleRowsInParallel(pool, numQueries, numRows) {
    console.log(`PG: Selecting ${numQueries} with each ${numRows} rows in parallel`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numQueries; i++) {
        const sql = `SELECT * FROM all_types WHERE col_bigint>$1 LIMIT ${numRows}`;
        promises.push(pool.query(sql, [getRandomInt(5000000)]));
        process.stdout.write('.');
    }
    console.log("Waiting for queries to finish");
    const allResults = await Promise.all(promises);
    allResults.forEach(result => {
        if (result.rows.length < 0 || result.rows.length > numRows) {
            console.log(`Unexpected row count: ${result.rows.length}`);
        }
    });
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time for executing ${numQueries} with ${numRows} rows each: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numQueries}ms`);
}

async function pgSelectAndUpdateRows(pool, numTransactions, numRowsPerTx) {
    console.log(`PG: Executing ${numTransactions} with each ${numRowsPerTx} rows per transaction`);
    const start = new Date();
    for (let i = 0; i < numTransactions; i++) {
        await pgRunTransaction(pool);
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time executing ${numTransactions} with ${numRowsPerTx} rows each: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numTransactions}ms`);
}

async function pgSelectAndUpdateRowsInParallel(pool, numTransactions, numRowsPerTx) {
    console.log(`PG: Executing ${numTransactions} with each ${numRowsPerTx} rows per transaction`);
    const start = new Date();
    const promises = [];
    for (let i = 0; i < numTransactions; i++) {
        promises.push(pgRunTransaction(pool));
        process.stdout.write('.');
    }
    process.stdout.write('\n');
    console.log("Waiting for transactions to finish");
    await Promise.all(promises);
    const end = new Date();
    const elapsed = end - start;
    console.log(`PG: Execution time executing ${numTransactions} with ${numRowsPerTx} rows each: ${elapsed}ms`);
    console.log(`PG: Avg execution time: ${elapsed/numTransactions}ms`);
}

async function pgRunTransaction(pool) {
    const client = await pool.connect()
    try {
        await client.query('BEGIN');
        const selectResult = await client.query('SELECT * FROM all_types WHERE col_bigint=$1', [getRandomInt(5000000)]);
        if (selectResult.rows.length === 1) {
            const updateResult = await client.query('UPDATE all_types SET col_float8=$1 WHERE col_bigint=$2', [
                Math.random(),
                selectResult.rows[0].col_bigint,
            ]);
            if (updateResult.rowCount !== 1) {
                console.error(`Unexpected update count: ${updateResult.rowCount}`);
            }
        }
        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

test().then(() => console.log('Finished'));
