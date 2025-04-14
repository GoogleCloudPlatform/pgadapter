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

import path from "path";
import {HttpClient} from "typed-rest-client/HttpClient";
import * as fs from "fs";
import gunzip from "gunzip-maybe";
import tar from "tar-fs";
import {checkPlatform} from "./binary";

async function installBinary() {
  const client = new HttpClient("pgadapter-nodejs");
  const url = determineUrl();
  const response = await client.get(url);
  const folder = path.join(__dirname, "..", "..", "bin");

  if (!fs.existsSync(folder)) {
    fs.mkdirSync(folder);
  }

  if (response.message.statusCode !== 200) {
    const err: Error = new Error(`Unexpected HTTP response: ${response.message.statusCode}`);
    err["httpStatusCode"] = response.message.statusCode;
    throw err;
  }

  console.log(`Downloading pgadapter from ${url}`);
  return new Promise((resolve, reject) => {
    const stream = response.message.pipe(gunzip()).pipe(tar.extract(folder));
    stream.on("error", (err) => reject(err));
    stream.on("close", () => {
      try { resolve(folder); } catch (err) {
        reject(err);
      }
    });
  });
}

function determineUrl(): string {
  checkPlatform(process.platform, process.arch);

  const host = "https://storage.googleapis.com/test-pgadapter-native-image";
  const version = require(path.join(__dirname, "..", "..", "package.json")).version;
  return `${host}/v${version}/pgadapter-${process.platform}-${process.arch}.tar.gz`;
}

(async () => {
  await installBinary();
})().catch(e => {
  console.error(e);
  process.exit(1);
});
