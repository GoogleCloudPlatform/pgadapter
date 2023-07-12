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
var assert = require("assert");
var mocha_1 = require("mocha");
var src_1 = require("../src");
mocha_1.describe('PGAdapter', function () {
    mocha_1.describe('instantiation', function () {
        mocha_1.it('should accept options', function () {
            assert.deepStrictEqual(new src_1.PGAdapter({ project: "my-project" }).getOptions(), { project: "my-project" });
            assert.deepStrictEqual(new src_1.PGAdapter({ instance: "my-instance" }).getOptions(), { instance: "my-instance" });
            assert.deepStrictEqual(new src_1.PGAdapter({ database: "my-database" }).getOptions(), { database: "my-database" });
        });
    });
    mocha_1.describe('command', function () {
        mocha_1.it('should generate project, instance and database command line arguments', function () {
            assert.strictEqual(new src_1.PGAdapter({ project: "my-project", instance: "my-instance" }).command(), "java -jar pgadapter.jar -p my-project -i my-instance");
        });
    });
    mocha_1.describe('start', function () {
        mocha_1.it('should start without any options', function () {
            var pg = new src_1.PGAdapter({});
            assert.strictEqual(pg.isRunning(), false);
            pg.start();
            assert.strictEqual(pg.isRunning(), true);
            // PGAdapter is automatically stopped when the main process stops.
        });
    });
});
//# sourceMappingURL=index.js.map