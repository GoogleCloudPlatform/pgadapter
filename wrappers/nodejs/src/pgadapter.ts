// Copyright 2025 Google LLC
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

import {checkPlatform} from "./install/binary";
import {ChildProcessWithoutNullStreams, spawn} from 'child_process';
import * as path from "path";
const tcpPortUsed = require('tcp-port-used');

export interface PGAdapterOptions {
  project?: string
  instance?: string
  database?: string

  port?: number
  credentials?: string
}

export interface StartupOptions {
  skipStartupProbe?: boolean,
  timeoutMs?: number,
  probeRetryMs?: number,

  platform?: NodeJS.Platform,
  arch?: NodeJS.Architecture,
}

export async function startPGAdapter(options?: PGAdapterOptions, startupOptions?: StartupOptions):
    Promise<ChildProcessWithoutNullStreams> {
  const platform = startupOptions?.platform || process.platform;
  const arch = startupOptions?.arch || process.arch;
  checkPlatform(platform, arch);

  const binary = path.join(__dirname, "..", "bin", `pgadapter-${platform}-${arch}`);
  const args: string[] = [];
  if (options?.project) {
    args.push("-p", options.project);
  }
  if (options?.instance) {
    args.push("-i", options.instance);
  }
  if (options?.database) {
    args.push("-d", options.database);
  }
  if (options?.port) {
    args.push("-s", `${options.port}`);
  }
  if (options?.credentials) {
    args.push("-c", options.credentials);
  }

  const pgAdapter = spawn(binary, args, {stdio: 'inherit'});
  await new Promise((resolve, reject) => {
    pgAdapter.on("spawn", resolve);
    pgAdapter.on("error", reject);
  });
  if (!startupOptions?.skipStartupProbe) {
    await tcpPortUsed.waitUntilUsed(
        options.port || 5432,
        startupOptions?.probeRetryMs || 100,
        startupOptions?.timeoutMs || 10000);
  }
  return pgAdapter;
}
