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

import * as assert from 'assert';
import {describe, it} from "mocha";
import {PGAdapter} from "../src";

describe('PGAdapter', () => {
  describe('instantiation', () => {
    it('should accept options', () => {
      assert.deepStrictEqual(new PGAdapter({project: "my-project"}).getOptions(), {project: "my-project"});
      assert.deepStrictEqual(new PGAdapter({instance: "my-instance"}).getOptions(), {instance: "my-instance"});
      assert.deepStrictEqual(new PGAdapter({database: "my-database"}).getOptions(), {database: "my-database"});
    });
  });

  describe('command', () => {
    it('should generate project, instance and database command line arguments', () => {
      assert.strictEqual(
        new PGAdapter({project: "my-project", instance: "my-instance"}).command(),
        "java -jar pgadapter.jar -p my-project -i my-instance",
      )
    });
  });

  describe('start', () => {
    it('should start without any options', async () => {
      const pg = new PGAdapter({});
      assert.strictEqual(pg.isRunning(), false);
      await pg.start();
      assert.strictEqual(pg.isRunning(), true);
      // PGAdapter is automatically stopped when the main process stops.
      // We therefore do not need to explicitly stop it. But as these tests
      // start multiple PGAdapter instances, we stop them as well to prevent
      // using too many resources.
      pg.stop();
    });

    it('should assign port', async () => {
      const pg = new PGAdapter({});
      await pg.start();
      assert(pg.getHostPort());
      pg.stop();
    });
  });
});
