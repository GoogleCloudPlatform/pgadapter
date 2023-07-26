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
exports.PGAdapter = exports.ExecutionEnvironment = void 0;
const child_process_1 = require("child_process");
const net_1 = require("net");
const Https = require("https");
const Fs = require("fs");
const util = require("util");
const path_1 = require("path");
const makeDir = require("make-dir");
const env_paths_1 = require("env-paths");
const tar_1 = require("tar");
const testcontainers_1 = require("testcontainers");
const paths = env_paths_1.default('pgadapter-downloads');
var ExecutionEnvironment;
(function (ExecutionEnvironment) {
    /**
     * Starts PGAdapter as a Java application. Requires Java to be installed on the local system.
     */
    ExecutionEnvironment[ExecutionEnvironment["Java"] = 0] = "Java";
    /**
     * Starts PGAdapter in a Docker test container. Requires Docker to be installed on the local system.
     * Starting PGAdapter in a Docker test container is only recommended for test and development
     * environments. Production environments should either start PGAdapter directly as a Java application,
     * or start a standalone PGAdapter Docker container (and not a test container).
     */
    ExecutionEnvironment[ExecutionEnvironment["Docker"] = 1] = "Docker";
})(ExecutionEnvironment = exports.ExecutionEnvironment || (exports.ExecutionEnvironment = {}));
/**
 * The PGAdapter class is a wrapper around the PGAdapter executable that allows you
 * to start and manage a PGAdapter instance directly from your Node.js code.
 *
 * @remarks
 * NOTE: PGAdapter requires that either Java or Docker is installed on the local system.
 *
 * @example
 * ```ts
 * import { Client } from "pg";
 *
 * const pg = new PGAdapter({
 *   project: "my-project",
 *   instance: "my-instance",
 *   credentialsFile: "/path/to/credentials.json",
 * });
 * await pg.start();
 * const port = pg.getHostPort();
 *
 * // You can now connect to Cloud Spanner using a standard PostgreSQL driver on `localhost:port`.
 * const client = new Client({host: "localhost", port: port, database: "knut-test-db"});
 * await client.connect();
 *
 * const res = await client.query('SELECT $1::text as message', ['Hello world!'])
 * console.log(res.rows[0].message) // Hello world!
 * await client.end()
 * ```
 */
class PGAdapter {
    constructor(options) {
        this.jarBaseDownloadUrl = "https://storage.googleapis.com/pgadapter-jar-releases/";
        this.latestJarFileName = "pgadapter.tar.gz";
        this.versionedJarFileName = "pgadapter-%s.tar.gz";
        this.dockerImage = "gcr.io/cloud-spanner-pg-adapter/pgadapter";
        this.options = Object.assign({}, options);
    }
    command() {
        const args = this._arguments();
        return "java -jar pgadapter.jar" + (args.length == 0 ? "" : " " + args.join(" "));
    }
    getOptions() {
        return Object.assign({}, this.options);
    }
    _arguments() {
        let res = [];
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
    }
    /**
     * Starts this instance of PGAdapter. The PGAdapter instance will start listening for
     * incoming PostgreSQL connections on the selected TCP port. A random TCP port will be
     * assigned to it if no TCP port was selected. The port where PGAdapter is listening
     * connections can be retrieved by calling {@link PGAdapter#getHostPort}.
     */
    async start() {
        if (this.pgadapter) {
            throw new Error("This PGAdapter instance has already been started");
        }
        if (this.options.executionEnvironment === ExecutionEnvironment.Java) {
            if (!this.isJavaAvailable()) {
                throw new Error("Java is not available on this system. Please install Java before trying to run PGAdapter as Java application. See https://adoptium.net/installation/ for instructions for how to install an open-source Java runtime version.");
            }
            await this.startJava();
        }
        else if (this.options.executionEnvironment === ExecutionEnvironment.Docker) {
            if (!this.isDockerAvailable()) {
                throw new Error("Docker is not available on this system. Please install Docker before trying to run PGAdapter in a Docker test container.");
            }
            await this.startDocker();
        }
        else {
            if (this.isJavaAvailable()) {
                await this.startJava();
            }
            else if (this.isDockerAvailable()) {
                await this.startDocker();
            }
            else {
                throw new Error("Neither Java nor Docker is available on this system. Please install Java or Docker before trying to run PGAdapter. See https://adoptium.net/installation/ for instructions for how to install an open-source Java runtime version.");
            }
        }
    }
    isJavaAvailable() {
        const childProcess = child_process_1.spawnSync("java", ["-version"]);
        return !childProcess.error;
    }
    isDockerAvailable() {
        const childProcess = child_process_1.spawnSync("docker", ["--version"]);
        return !childProcess.error;
    }
    async startDocker() {
        let image = this.dockerImage;
        if (this.options.version) {
            image += ":" + this.options.version;
        }
        let container = new testcontainers_1.GenericContainer(image);
        if (!this.options.version) {
            container = container.withPullPolicy(new testcontainers_1.AlwaysPullPolicy());
        }
        container = container.withCommand(this._arguments());
        const startedContainer = await container.start();
    }
    async startJava() {
        const jarFile = await this.downloadJar();
        let args = ["-jar", jarFile];
        args.push(...this._arguments());
        let port;
        if (this.options.port) {
            port = Promise.resolve(this.options.port);
        }
        else {
            port = new Promise((resolve, reject) => {
                const server = net_1.createServer(sock => {
                    sock.end();
                });
                server.listen(() => {
                    if (!server.address() || !server.address().port) {
                        reject(new Error("No available address returned for socket"));
                    }
                    resolve(server.address().port);
                    server.close();
                });
            });
        }
        this.port = await port;
        args.push("-s", String(this.port));
        this.pgadapter = child_process_1.spawn("java", args, {
            detached: true,
            stdio: "ignore",
            windowsHide: true,
        });
        this.pgadapter.unref();
        process.on("exit", () => {
            if (this.pgadapter) {
                this.pgadapter.kill("SIGINT");
                this.pgadapter = undefined;
            }
        });
        for (let n = 0; n < 100; n++) {
            try {
                await this.checkPGAdapterAvailable(this.port);
                return;
            }
            catch (e) {
                // ignore and retry
                await this.sleep(50);
            }
        }
        throw new Error("PGAdapter failed to start successfully");
    }
    /** Checks whether PGAdapter is available on the given port number. */
    async checkPGAdapterAvailable(port) {
        return new Promise((resolve, reject) => {
            const socket = new net_1.Socket();
            const onError = async () => {
                socket.destroy();
                reject(new Error("PGAdapter is not available"));
            };
            socket.setTimeout(20);
            socket.once("error", onError);
            socket.once("timeout", onError);
            socket.connect(port, "localhost", () => {
                socket.end();
                resolve();
            });
        });
    }
    sleep(time) {
        return new Promise(resolve => setTimeout(resolve, time));
    }
    async downloadJar(version, targetDir) {
        let url = this.jarBaseDownloadUrl;
        let fileName;
        if (version) {
            fileName = util.format(this.versionedJarFileName, version);
        }
        else {
            fileName = this.latestJarFileName;
        }
        url += fileName;
        if (!targetDir) {
            const dir = version ? util.format("pgadapter-%s", version) : "pgadapter-latest";
            targetDir = path_1.join(paths.cache, dir);
        }
        await makeDir(targetDir);
        const target = path_1.join(targetDir, fileName);
        await this.downloadFile(url, target);
        await tar_1.extract({ file: target, cwd: targetDir, newer: true });
        return path_1.join(targetDir, "pgadapter.jar");
    }
    /** Downloads the file at the given url to the specified target file. */
    async downloadFile(url, targetFile) {
        return await new Promise((resolve, reject) => {
            Https.get(url, response => {
                var _a;
                const code = (_a = response.statusCode) !== null && _a !== void 0 ? _a : 0;
                if (code >= 400) {
                    return reject(new Error(response.statusMessage));
                }
                // Follow any redirects.
                if (code > 300 && code < 400 && !!response.headers.location) {
                    return resolve(this.downloadFile(response.headers.location, targetFile));
                }
                // Check if the target already exists and whether it is up-to-date.
                this.downloadExistsAndIsUpToDate(response, targetFile)
                    .then(exists => {
                    if (exists) {
                        response.destroy();
                        return resolve();
                    }
                    // Save the download to disk.
                    const fileWriter = Fs
                        .createWriteStream(targetFile)
                        .on("finish", () => {
                        resolve();
                    });
                    response.pipe(fileWriter);
                });
            }).on("error", error => {
                reject(error);
            });
        });
    }
    async downloadExistsAndIsUpToDate(response, targetFile) {
        return await new Promise((resolve, reject) => {
            Fs.stat(targetFile, (err, stats) => {
                if (err) {
                    resolve(false);
                    return;
                }
                const lastModifiedString = response.headers["last-modified"];
                if (!lastModifiedString) {
                    resolve(false);
                    return;
                }
                const lastModified = new Date(lastModifiedString);
                resolve(stats.mtime >= lastModified);
            });
        });
    }
    /**
     * @returns The TCP port where this PGAdapter instance is listening for incoming
     * connections. You can connect to this TCP port using standard PostgreSQL clients
     * and drivers.
     */
    getHostPort() {
        if (!this.isRunning() || !this.port) {
            throw new Error("This PGAdapter instance is not running");
        }
        return this.port;
    }
    /**
     * @returns true if this instance of PGAdapter has been started.
     */
    isRunning() {
        return this.pgadapter !== undefined;
    }
    /**
     * Stops this PGAdapter instance. The PGAdapter instance is also automatically stopped
     * when your application is shut down gracefully.
     */
    stop() {
        if (!this.pgadapter) {
            throw new Error("This PGAdapter instance is not running");
        }
        this.pgadapter.kill("SIGINT");
        this.pgadapter = undefined;
    }
}
exports.PGAdapter = PGAdapter;
//# sourceMappingURL=index.js.map