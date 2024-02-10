// Copyright 2024 Google LLC
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

import {sequelize, shutdownSequelize, startSequelize} from './init'
import {initSinger, Singer} from '../models/singer';

async function main() {
  await startSequelize();

  initSinger();
  // Create a new singer record.
  const singer = await Singer.create({ firstName: "Jane", lastName: "Doe" });
  console.log(`Created a new singer record with ID ${singer.id}`);

  await shutdownSequelize();
}


(async () => {
  await main();
})().catch(e => {
  console.error(e);
});
