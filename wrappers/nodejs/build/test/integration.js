"use strict";
/*!
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
Object.defineProperty(exports, "__esModule", { value: true });
const mocha_1 = require("mocha");
const src_1 = require("../src");
const pg_1 = require("pg");
const assert = require("assert");
mocha_1.describe('PGAdapter', () => {
    mocha_1.describe('connect', () => {
        mocha_1.it('should connect to Cloud Spanner using Java', async () => {
            const pg = new src_1.PGAdapter({
                executionEnvironment: src_1.ExecutionEnvironment.Java,
                project: "appdev-soda-spanner-staging",
                instance: "knut-test-ycsb",
                credentialsFile: "/Users/loite/Downloads/appdev-soda-spanner-staging.json",
            });
            await runConnectionTest(pg);
        });
        mocha_1.it('should connect to Cloud Spanner using Docker', async () => {
            const pg = new src_1.PGAdapter({
                executionEnvironment: src_1.ExecutionEnvironment.Docker,
                project: "appdev-soda-spanner-staging",
                instance: "knut-test-ycsb",
                credentialsFile: "/Users/loite/Downloads/appdev-soda-spanner-staging.json",
            });
            await runConnectionTest(pg);
        });
    });
});
async function runConnectionTest(pg) {
    await pg.start();
    const port = pg.getHostPort();
    const client = new pg_1.Client({ host: "localhost", port: port, database: "knut-test-db" });
    await client.connect();
    const res = await client.query('SELECT $1::text as message', ['Hello world!']);
    assert.strictEqual(res.rows[0].message, "Hello world!");
    await client.end();
    await pg.stop();
}
//# sourceMappingURL=integration.js.map