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
export declare enum ExecutionEnvironment {
    /**
     * Starts PGAdapter as a Java application. Requires Java to be installed on the local system.
     */
    Java = 0,
    /**
     * Starts PGAdapter in a Docker test container. Requires Docker to be installed on the local system.
     * Starting PGAdapter in a Docker test container is only recommended for test and development
     * environments. Production environments should either start PGAdapter directly as a Java application,
     * or start a standalone PGAdapter Docker container (and not a test container).
     */
    Docker = 1
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
    executionEnvironment?: ExecutionEnvironment;
    /**
     * The PGAdapter version to start. By default, the latest version will be started.
     */
    version?: string;
    /**
     * Project is the Google Cloud project that PGAdapter should connect to.
     *
     * If project is not set, then the PostgreSQL client that connects to PGAdapter
     * must provide a fully qualified database name in the format
     * `projects/my-project/instances/my-instance/databases/my-database`.
     */
    project?: string;
    /**
     * Instance is the Google Cloud Spanner instance that PGAdapter should connect to.
     *
     * If instance is not set, then the PostgreSQL client that connects to PGAdapter
     * must provide a fully qualified database name in the format
     * `projects/my-project/instances/my-instance/databases/my-database`.
     */
    instance?: string;
    /**
     * Database is the Google Cloud Spanner database that PGAdapter should connect to.
     *
     * If database is not set, then the database that is given in the PostgreSQL connection
     * URL will be used. If both `project` and `instance` have been set, then the database
     * name can be a simple database id. The connection URL must use a fully qualified
     * database name if either `project` or `instance` has not been set in these options.
     */
    database?: string;
    /**
     * The local TCP port that PGAdapter should use to listen for incoming connections.
     *
     * Leave it undefined or set it to zero to use a random port. The port that was chosen
     * can be retrieved by calling {@link PGAdapter#getHostPort} after it has been started.
     */
    port?: number;
    /**
     * Specifies the credentials file, such as a user account file or a service account key
     * file, that should be used by PGAdapter when connecting to Cloud Spanner.
     *
     * If this is not set, then PGAdapter will use the default credentials in the runtime
     * environment.
     */
    credentialsFile?: string;
    /**
     * Requires PostgreSQL clients to authenticate when connecting to PGAdapter. The PostgreSQL
     * client must then include the Google Cloud credentials as a JSON string in the password
     * message when connecting to Cloud Spanner.
     *
     * @remarks
     * This property may only be enabled when no credentials file has been specified.
     */
    requireAuthentication?: boolean;
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
declare class PGAdapter {
    private readonly jarBaseDownloadUrl;
    private readonly latestJarFileName;
    private readonly versionedJarFileName;
    private readonly dockerImage;
    private readonly options;
    private pgadapter?;
    private port?;
    constructor(options: Options);
    command(): string;
    getOptions(): Options;
    private _arguments;
    /**
     * Starts this instance of PGAdapter. The PGAdapter instance will start listening for
     * incoming PostgreSQL connections on the selected TCP port. A random TCP port will be
     * assigned to it if no TCP port was selected. The port where PGAdapter is listening
     * connections can be retrieved by calling {@link PGAdapter#getHostPort}.
     */
    start(): Promise<void>;
    private isJavaAvailable;
    private isDockerAvailable;
    private startDocker;
    private startJava;
    /** Checks whether PGAdapter is available on the given port number. */
    private checkPGAdapterAvailable;
    private sleep;
    private downloadJar;
    /** Downloads the file at the given url to the specified target file. */
    private downloadFile;
    private downloadExistsAndIsUpToDate;
    /**
     * @returns The TCP port where this PGAdapter instance is listening for incoming
     * connections. You can connect to this TCP port using standard PostgreSQL clients
     * and drivers.
     */
    getHostPort(): number;
    /**
     * @returns true if this instance of PGAdapter has been started.
     */
    isRunning(): boolean;
    /**
     * Stops this PGAdapter instance. The PGAdapter instance is also automatically stopped
     * when your application is shut down gracefully.
     */
    stop(): void;
}
export { PGAdapter };
