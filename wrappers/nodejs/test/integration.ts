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

import {describe, it} from "mocha";
import {PGAdapter} from "../src";
import { Client } from "pg";

describe('PGAdapter', () => {
  describe('connect', () => {
    it('should connect to Cloud Spanner', async () => {
      const pg = new PGAdapter({
        project: "appdev-soda-spanner-staging",
        instance: "knut-test-ycsb",
        credentialsFile: "/Users/loite/Downloads/appdev-soda-spanner-staging.json",
      });
      await pg.start();
      const port = pg.getHostPort();

      const client = new Client({host: "localhost", port: port, database: "knut-test-db"});
      await client.connect();

      const res = await client.query('SELECT $1::text as message', ['Hello world!'])
      console.log(res.rows[0].message) // Hello world!
      await client.end()
    });
  });
});
