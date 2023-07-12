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

import {ChildProcess, spawn} from "child_process";
import {createServer} from "net";

export interface Options {
  project?: string
  instance?: string
  database?: string
  port?: number
  credentialsFile?: string
  requireAuthentication?: boolean
}

class PGAdapter {
  private readonly options: Options;
  private pgadapter?: ChildProcess;

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

  private _arguments(): string[] {
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
    if (this.options.credentialsFile) {
      res.push("-c", this.options.credentialsFile);
    }
    if (this.options.requireAuthentication) {
      res.push("-a");
    }
    return res;
  }

  start(): void {
    if (this.pgadapter) {
      throw new Error("This PGAdapter instance has already been started");
    }
    this._startJava();
  }

  private _startJava() {
    let args = ["-jar", "/Users/loite/Library/Caches/pgadapter-downloads/pgadapter-latest/pgadapter.jar"];
    args.push(...this._arguments());
    if (this.options.port) {
      args.push("-s", String(this.options.port));
    } else {
      const server = createServer(sock => {
        sock.end();
      });
      server.listen(0);
      args.push("-s", "0");
    }
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
  }

  getHostPort(): number {
    if (!this.isRunning()) {
      throw new Error("This PGAdapter instance is not running");
    }
    if (this.options.port) {
      return this.options.port;
    }
    return 0;
  }

  isRunning(): boolean {
    return this.pgadapter !== undefined;
  }

  stop(): void {
    if (!this.pgadapter) {
      throw new Error("This PGAdapter instance is not running");
    }
    this.pgadapter.kill("SIGINT");
    this.pgadapter = undefined;
  }
}


export {PGAdapter}
