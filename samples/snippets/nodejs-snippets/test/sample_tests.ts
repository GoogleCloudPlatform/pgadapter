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

import {
    TestContainer,
    StartedTestContainer,
    GenericContainer,
    PullPolicy
} from "testcontainers";
import createTables from "../src/create_tables";
import createConnection from "../src/create_connection"
import writeDataWithDml from "../src/write_data_with_dml"
import writeDataWithDmlBatch from "../src/write_data_with_dml_batch"

const container: TestContainer = new GenericContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
    .withExposedPorts(5432)
    .withPullPolicy(PullPolicy.alwaysPull());

describe('running samples', () => {
    let startedTestContainer: StartedTestContainer;
    const log = console.log;

    beforeAll(async () => {
        startedTestContainer = await container.start();
        console.log = jest.fn();
    }, 30000);

    afterAll(async () => {
        console.log = log;
        if (startedTestContainer) {
            await startedTestContainer.stop({remove: true});
        }
    }, 30000);

    test('create tables', async () => {
        await createTables(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        expect(console.log).toHaveBeenCalledWith("Created Singers & Albums tables in database: [example-db]");
    }, 30000);
    test('create connection', async () => {
        await createConnection(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        expect(console.log).toHaveBeenCalledWith("Greeting from Cloud Spanner PostgreSQL: Hello world!");
    }, 30000);
    test('write data with DML', async () => {
        await writeDataWithDml(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        expect(console.log).toHaveBeenCalledWith("4 records inserted");
    }, 30000);
    test('execute DML batch', async () => {
        await writeDataWithDmlBatch(startedTestContainer.getHost(), startedTestContainer.getMappedPort(5432), "example-db");
        expect(console.log).toHaveBeenCalledWith("3 records inserted");
    }, 30000);
});
