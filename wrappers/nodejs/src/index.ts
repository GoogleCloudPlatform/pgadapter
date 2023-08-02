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

import {ChildProcess, spawn, spawnSync} from "child_process";
import {AddressInfo, createServer, Socket} from "net";
import * as Https from "https";
import * as Fs from "fs";
import * as util from "util";
import {join} from "path";
import * as makeDir from "make-dir";
import envPaths from "env-paths";
import {IncomingMessage} from "http";
import {extract} from "tar";
import {
  TestContainer,
  StartedTestContainer,
  GenericContainer, AlwaysPullPolicy,
} from "testcontainers";


const paths = envPaths('pgadapter-downloads');

export enum ExecutionEnvironment {
  /**
   * Starts PGAdapter as a Java application. Requires Java to be installed on the local system.
   */
  Java,

  /**
   * Starts PGAdapter in a Docker test container. Requires Docker to be installed on the local system.
   * Starting PGAdapter in a Docker test container is only recommended for test and development
   * environments. Production environments should either start PGAdapter directly as a Java application,
   * or start a standalone PGAdapter Docker container (and not a test container).
   */
  Docker,
}

/**
 * Options contains the different startup options for PGAdapter.
 */
export interface Options {
  /**
   * The execution environment that should be used to start PGAdapter.
   *
   * If none is specified, then it will default to Java if Java is
   * available on this system. It will then fall back to Docker if
   * that is available on this system.
   *
   * If neither Java nor Docker are available on this system, an error
   * will be thrown.
   */
  executionEnvironment?: ExecutionEnvironment
  /**
   * The PGAdapter version to start. By default, the latest version will be started.
   */
  version?: string

  /**
   * Project is the Google Cloud project that PGAdapter should connect to.
   *
   * If project is not set, then the PostgreSQL client that connects to PGAdapter
   * must provide a fully qualified database name in the format
   * `projects/my-project/instances/my-instance/databases/my-database`.
   */
  project?: string
  /**
   * Instance is the Google Cloud Spanner instance that PGAdapter should connect to.
   *
   * If instance is not set, then the PostgreSQL client that connects to PGAdapter
   * must provide a fully qualified database name in the format
   * `projects/my-project/instances/my-instance/databases/my-database`.
   */
  instance?: string
  /**
   * Database is the Google Cloud Spanner database that PGAdapter should connect to.
   *
   * If database is not set, then the database that is given in the PostgreSQL connection
   * URL will be used. If both `project` and `instance` have been set, then the database
   * name can be a simple database id. The connection URL must use a fully qualified
   * database name if either `project` or `instance` has not been set in these options.
   */
  database?: string

  /**
   * The local TCP port that PGAdapter should use to listen for incoming connections.
   *
   * Leave it undefined or set it to zero to use a random port. The port that was chosen
   * can be retrieved by calling {@link PGAdapter#getHostPort} after it has been started.
   */
  port?: number

  /**
   * Specifies the credentials file, such as a user account file or a service account key
   * file, that should be used by PGAdapter when connecting to Cloud Spanner.
   *
   * If this is not set, then PGAdapter will use the default credentials in the runtime
   * environment.
   */
  credentialsFile?: string
  /**
   * Requires PostgreSQL clients to authenticate when connecting to PGAdapter. The PostgreSQL
   * client must then include the Google Cloud credentials as a JSON string in the password
   * message when connecting to Cloud Spanner.
   *
   * @remarks
   * This property may only be enabled when no credentials file has been specified.
   */
  requireAuthentication?: boolean
}

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
  private readonly jarBaseDownloadUrl = "https://storage.googleapis.com/pgadapter-jar-releases/"
  private readonly latestJarFileName = "pgadapter.tar.gz"
  private readonly versionedJarFileName = "pgadapter-%s.tar.gz"

  private readonly dockerImage = "gcr.io/cloud-spanner-pg-adapter/pgadapter";

  private readonly options: Options;
  private pgadapter?: ChildProcess;
  private container?: StartedTestContainer;
  private port?: number;

  constructor(options:Options) {
    this.options = Object.assign({}, options);
  }

  command(): string {
    const args = this._arguments();
    return "java -jar pgadapter.jar" + (args.length == 0 ? "" : " " + args.join(" "));
  }

  getOptions(): Options {
    return Object.assign({}, this.options);
  }

  private _arguments(execEnv?: ExecutionEnvironment): string[] {
    let res: string[] = [];
    if (this.options.project) {
      res.push("-p", this.options.project);
    }
    if (this.options.instance) {
      res.push("-i", this.options.instance);
    }
    if (this.options.database) {
      res.push("-d", this.options.database);
    }
    if (execEnv === ExecutionEnvironment.Docker) {
      res.push("-c", "/credentials.json");
    } else if (this.options.credentialsFile) {
      res.push("-c", this.options.credentialsFile);
    }
    if (this.options.requireAuthentication) {
      res.push("-a");
    }
    if (execEnv === ExecutionEnvironment.Docker) {
      res.push("-x");
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
    } else if (this.options.executionEnvironment === ExecutionEnvironment.Docker) {
      if (!this.isDockerAvailable()) {
        throw new Error("Docker is not available on this system. Please install Docker before trying to run PGAdapter in a Docker test container.");
      }
      await this.startDocker();
    } else {
      if (this.isJavaAvailable()) {
        await this.startJava();
      } else if (this.isDockerAvailable()) {
        await this.startDocker();
      } else {
        throw new Error("Neither Java nor Docker is available on this system. Please install Java or Docker before trying to run PGAdapter. See https://adoptium.net/installation/ for instructions for how to install an open-source Java runtime version.");
      }
    }
  }

  private isJavaAvailable(): boolean {
    const childProcess = spawnSync("java", ["-version"]);
    return !childProcess.error;
  }

  private isDockerAvailable(): boolean {
    const childProcess = spawnSync("docker", ["--version"]);
    return !childProcess.error;
  }

  private async startDocker() {
    let image = this.dockerImage;
    if (this.options.version) {
      image += ":" + this.options.version;
    }
    let container: TestContainer = new GenericContainer(image);
    if (!this.options.version) {
      container = container.withPullPolicy(new AlwaysPullPolicy());
    }
    if (this.options.credentialsFile) {
      container = container.withCopyFilesToContainer([{
        source: this.options.credentialsFile,
        target: "/credentials.json",
        mode: 700
      }])
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      container = container.withCopyFilesToContainer([{
        source: process.env.GOOGLE_APPLICATION_CREDENTIALS,
        target: "/credentials.json",
        mode: 700
      }])
    } else {
      throw new Error("Running PGAdapter in Docker requires a credentials file. Set options.credentialsFile to a valid user or service account file.");
    }
    if (this.options.port) {
      container = container.withExposedPorts({host: this.options.port, container: 5432});
    } else {
      container = container.withExposedPorts(5432);
    }
    container = container.withCommand(this._arguments(ExecutionEnvironment.Docker));
    this.container = await container.start();
    process.on("exit", () => {
      if (this.container) {
        this.container.stop();
        this.container = undefined;
      }
    });
    this.port = this.container.getMappedPort(5432);
  }

  private async startJava() {
    const jarFile = await this.downloadJar();
    let args = ["-jar", jarFile];
    args.push(...this._arguments(ExecutionEnvironment.Java));
    let port: Promise<number>;
    if (this.options.port) {
      port = Promise.resolve(this.options.port);
    } else {
      port = new Promise((resolve, reject) => {
        const server = createServer(sock => {
          sock.end();
        });
        server.listen(() => {
          if (!server.address() || !(server.address() as any).port) {
            reject(new Error("No available address returned for socket"));
          }
          resolve((server.address()! as AddressInfo).port);
          server.close();
        });
      });
    }
    this.port = await port;
    args.push("-s", String(this.port));
    this.pgadapter = spawn("java", args,
      {
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
    for (let n=0; n<100; n++) {
      try {
        await this.checkPGAdapterAvailable(this.port);
        return;
      } catch (e) {
        // ignore and retry
        await this.sleep(20);
      }
    }
    throw new Error("PGAdapter failed to start successfully");
  }

  /** Checks whether PGAdapter is available on the given port number. */
  private async checkPGAdapterAvailable(port: number) {
    return new Promise<void>((resolve, reject) => {
      const socket = new Socket();
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

  private sleep(time: number) {
    return new Promise<void>(resolve => setTimeout(resolve, time));
  }

  private async downloadJar(version?: string, targetDir?: string): Promise<string> {
    let url = this.jarBaseDownloadUrl;
    let fileName: string;
    if (version) {
      fileName = util.format(this.versionedJarFileName, version);
    } else {
      fileName = this.latestJarFileName;
    }
    url += fileName;
    if (!targetDir) {
      const dir = version ? util.format("pgadapter-%s", version) : "pgadapter-latest";
      targetDir = join(paths.cache, dir);
    }
    await makeDir(targetDir);
    const target = join(targetDir, fileName);
    await this.downloadFile(url, target);
    await extract({file: target, cwd: targetDir, newer: true});
    return join(targetDir, "pgadapter.jar");
  }

  /** Downloads the file at the given url to the specified target file. */
  private async downloadFile(url: string, targetFile: string): Promise<void> {
    return await new Promise((resolve, reject) => {
      Https.get(url, response => {
        const code = response.statusCode ?? 0
        if (code >= 400) {
          return reject(new Error(response.statusMessage))
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
    })
  }

  private async downloadExistsAndIsUpToDate(response: IncomingMessage, targetFile: string): Promise<boolean> {
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
        const lastModified = new Date(lastModifiedString!);
        resolve(stats.mtime >= lastModified);
      });
    });
  }

  /**
   * @returns The TCP port where this PGAdapter instance is listening for incoming
   * connections. You can connect to this TCP port using standard PostgreSQL clients
   * and drivers.
   */
  getHostPort(): number {
    if (!this.isRunning() || !this.port) {
      throw new Error("This PGAdapter instance is not running");
    }
    return this.port;
  }

  /**
   * @returns true if this instance of PGAdapter has been started.
   */
  isRunning(): boolean {
    return this.pgadapter !== undefined || (this.container !== undefined);
  }

  /**
   * Stops this PGAdapter instance. The PGAdapter instance is also automatically stopped
   * when your application is shut down gracefully.
   */
  async stop(): Promise<void> {
    if (!this.pgadapter && !this.container) {
      throw new Error("This PGAdapter instance is not running");
    }
    if (this.pgadapter) {
      this.pgadapter.kill("SIGINT");
      this.pgadapter = undefined;
    }
    if (this.container) {
      await this.container.stop();
      this.container = undefined;
    }
  }
}


export {PGAdapter}
