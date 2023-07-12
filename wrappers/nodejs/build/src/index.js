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
exports.PGAdapter = void 0;
var child_process_1 = require("child_process");
var net_1 = require("net");
var PGAdapter = /** @class */ (function () {
    function PGAdapter(options) {
        this.options = Object.assign({}, options);
    }
    PGAdapter.prototype.command = function () {
        var args = this._arguments();
        return "java -jar pgadapter.jar" + (args.length == 0 ? "" : " " + args.join(" "));
    };
    PGAdapter.prototype.getOptions = function () {
        return Object.assign({}, this.options);
    };
    PGAdapter.prototype._arguments = function () {
        var res = [];
        if (this.options.project) {
            res.push("-p", this.options.project);
        }
        if (this.options.instance) {
            res.push("-i", this.options.instance);
        }
        if (this.options.database) {
            res.push("-d", this.options.database);
        }
        if (this.options.credentialsFile) {
            res.push("-c", this.options.credentialsFile);
        }
        if (this.options.requireAuthentication) {
            res.push("-a");
        }
        return res;
    };
    PGAdapter.prototype.start = function () {
        if (this.pgadapter) {
            throw new Error("This PGAdapter instance has already been started");
        }
        this._startJava();
    };
    PGAdapter.prototype._startJava = function () {
        var _this = this;
        var args = ["-jar", "/Users/loite/Library/Caches/pgadapter-downloads/pgadapter-latest/pgadapter.jar"];
        args.push.apply(args, this._arguments());
        if (this.options.port) {
            args.push("-s", String(this.options.port));
        }
        else {
            var server = net_1.createServer(function (sock) {
                sock.end();
            });
            server.listen(0);
            args.push("-s", "0");
        }
        this.pgadapter = child_process_1.spawn("java", args, {
            detached: true,
            stdio: "ignore",
            windowsHide: true,
        });
        this.pgadapter.unref();
        process.on("exit", function () {
            if (_this.pgadapter) {
                _this.pgadapter.kill("SIGINT");
                _this.pgadapter = undefined;
            }
        });
    };
    PGAdapter.prototype.getHostPort = function () {
        if (!this.isRunning()) {
            throw new Error("This PGAdapter instance is not running");
        }
        if (this.options.port) {
            return this.options.port;
        }
        return 0;
    };
    PGAdapter.prototype.isRunning = function () {
        return this.pgadapter !== undefined;
    };
    PGAdapter.prototype.stop = function () {
        if (!this.pgadapter) {
            throw new Error("This PGAdapter instance is not running");
        }
        this.pgadapter.kill("SIGINT");
        this.pgadapter = undefined;
    };
    return PGAdapter;
}());
exports.PGAdapter = PGAdapter;
//# sourceMappingURL=index.js.map