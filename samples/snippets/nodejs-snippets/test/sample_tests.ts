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

import { describe, before, after, test, beforeEach } from 'node:test';
import assert from 'node:assert';
import {
    TestContainer,
    StartedTestContainer,
    GenericContainer,
    PullPolicy
} from "testcontainers";
import createTables from "../src/create_tables";
import createConnection from "../src/create_connection";
import writeDataWithDml from "../src/write_data_with_dml";
import writeDataWithDmlBatch from "../src/write_data_with_dml_batch";
import writeDataWithCopy from "../src/write_data_with_copy";
import queryData from "../src/query_data";
import queryWithParameter from "../src/query_data_with_parameter";
import addColumn from "../src/add_column";
import ddlBatch from "../src/ddl_batch";
import updateDataWithCopy from "../src/update_data_with_copy";
import queryDataWithNewColumn from "../src/query_data_with_new_column";
import writeWithTransactionUsingDml from "../src/update_data_with_transaction";
import tags from "../src/tags";
import readOnlyTransaction from "../src/read_only_transaction";
import dataBoost from "../src/data_boost";
import partitionedDml from "../src/partitioned_dml";

const container: TestContainer = new GenericContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
    .withExposedPorts(5432)
    .withPullPolicy(PullPolicy.alwaysPull());

describe('running samples', () => {
    let startedTestContainer: StartedTestContainer;
    const log = console.log;
    const loggedLines: string[] = [];

    before(async () => {
        startedTestContainer = await container.start();
        console.log = (...args: any[]) => {
            loggedLines.push(args.map(arg => typeof arg === 'string' ? arg : JSON.stringify(arg)).join(' '));
        };
    });

    after(async () => {
        console.log = log;
        if (startedTestContainer) {
            await startedTestContainer.stop({remove: true});
        }
    });

    beforeEach(() => {
        loggedLines.length = 0;
    });

    test('create tables', { timeout: 30000 }, async () => {
        await createTables(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Created Singers & Albums tables in database: [example-db]"));
    });
    test('create connection', { timeout: 30000 }, async () => {
        await createConnection(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Greeting from Cloud Spanner PostgreSQL: Hello world!"));
    });
    test('write data with DML', { timeout: 30000 }, async () => {
        await writeDataWithDml(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("4 records inserted"));
    });
    test('execute DML batch', { timeout: 30000 }, async () => {
        await writeDataWithDmlBatch(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("3 records inserted"));
    });
    test('copy from stdin', { timeout: 30000 }, async () => {
        await writeDataWithCopy(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Copied 5 singers"));
        assert(loggedLines.includes("Copied 5 albums"));
    });
    test('query data', { timeout: 30000 }, async () => {
        await queryData(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("1 2 Go, Go, Go"));
        assert(loggedLines.includes("2 2 Forever Hold Your Peace"));
        assert(loggedLines.includes("1 1 Total Junk"));
        assert(loggedLines.includes("2 1 Green"));
        assert(loggedLines.includes("2 3 Terrified"));
    });
    test('query with parameter', { timeout: 30000 }, async () => {
        await queryWithParameter(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("12 Melissa Garcia"));
    });
    test('add column', { timeout: 30000 }, async () => {
        await addColumn(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Added marketing_budget column"));
    });
    test('ddl batch', { timeout: 30000 }, async () => {
        await ddlBatch(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Added venues and concerts tables"));
    });
    test('update data', { timeout: 30000 }, async () => {
        await updateDataWithCopy(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Updated 2 albums"));
    });
    test('query data with new column', { timeout: 30000 }, async () => {
        await queryDataWithNewColumn(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("1 1 100000"));
        assert(loggedLines.includes("1 2 null"));
        assert(loggedLines.includes("2 1 null"));
        assert(loggedLines.includes("2 2 500000"));
        assert(loggedLines.includes("2 3 null"));
    });
    test('update data with transaction', { timeout: 30000 }, async () => {
        await writeWithTransactionUsingDml(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Transferred marketing budget from Album 2 to Album 1"));
    });
    test('transaction and statement tags', { timeout: 30000 }, async () => {
        await tags(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Reduced marketing budget"));
    });
    test('read-only transaction', { timeout: 30000 }, async () => {
        await readOnlyTransaction(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("1 1 Total Junk"));
        assert(loggedLines.includes("1 2 Go, Go, Go"));
        assert(loggedLines.includes("2 1 Green"));
        assert(loggedLines.includes("2 2 Forever Hold Your Peace"));
        assert(loggedLines.includes("2 3 Terrified"));
    });
    test('data boost', { timeout: 30000 }, async () => {
        await dataBoost(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("2 Catalina Smith"));
        assert(loggedLines.includes("4 Lea Martin"));
        assert(loggedLines.includes("12 Melissa Garcia"));
        assert(loggedLines.includes("14 Jacqueline Long"));
        assert(loggedLines.includes("16 Sarah Wilson"));
        assert(loggedLines.includes("18 Maya Patel"));
        assert(loggedLines.includes("1 Marc Richards"));
        assert(loggedLines.includes("3 Alice Trentor"));
        assert(loggedLines.includes("5 David Lomond"));
        assert(loggedLines.includes("13 Russel Morales"));
        assert(loggedLines.includes("15 Dylan Shaw"));
        assert(loggedLines.includes("17 Ethan Miller"));
    });
    test('partitioned DML', { timeout: 30000 }, async () => {
        await partitionedDml(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        assert(loggedLines.includes("Updated at least 3 albums"));
    });
});
