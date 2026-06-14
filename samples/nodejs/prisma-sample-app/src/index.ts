// Copyright 2023 Google LLC
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

import {
  createPrismaClient,
  createRandomSingersAndAlbumsAndTracks, createTicketSale,
  createVenueAndConcertInTransaction,
  deleteExistingData, deployMigrations, printAlbumsReleasedBefore1900,
  printSingersAndAlbums, staleRead, updateVenueDescription
} from "./sample";
import {GenericContainer, PullPolicy, StartedTestContainer, TestContainer} from "testcontainers";
import fs from "fs";
import path from "path";

// Load environment variables from .env file.
// Prior to Prisma 7, Prisma Client automatically loaded .env files,
// but with driver adapters, we must load them manually.
function loadEnv() {
  try {
    const envPath = path.resolve(__dirname, "../.env");
    if (fs.existsSync(envPath)) {
      const envFile = fs.readFileSync(envPath, "utf-8");
      for (const line of envFile.split("\n")) {
        const match = line.match(/^\s*([\w.-]+)\s*=\s*(.*)?\s*$/);
        if (match) {
          const key = match[1];
          let value = match[2] || "";
          if (value.startsWith('"') && value.endsWith('"')) {
            value = value.slice(1, -1);
          } else if (value.startsWith("'") && value.endsWith("'")) {
            value = value.slice(1, -1);
          }
          if (!process.env[key]) {
            process.env[key] = value;
          }
        }
      }
    }
  } catch (e) {
    // ignore
  }
}
loadEnv();

console.log("Running sample application...");

runSample()
  .then(async () => {
    console.log("Successfully executed sample application");
  })
  .catch(async (e) => {
    console.error(e);
    process.exit(1);
  });

async function runSample() {
  let pgAdapter: StartedTestContainer = null;
  if (process.env.AUTO_START_PGADAPTER && process.env.AUTO_START_PGADAPTER.toLowerCase() == 'true') {
    // Start PGAdapter and the Spanner emulator in a Docker container.
    // Using a TestContainer to run PGAdapter is OK in development and test, but for production, it is
    // recommended to run PGAdapter as a side-car container.
    // See https://github.com/GoogleCloudPlatform/pgadapter/tree/postgresql-dialect/samples/cloud-run/nodejs
    // for a sample.
    pgAdapter = await startPGAdapter();
    const port = pgAdapter.getMappedPort(5432);
    console.log(`PGAdapter started on port ${port}`);

    // Dynamically set the DATABASE_URL environment variable to point to the PGAdapter instance
    // that was started.
    process.env.DATABASE_URL = `postgresql://127.0.0.1:${port}/prisma-sample?options=-c%20spanner.well_known_client=prisma%20-c%20timezone=UTC`;
  }
  
  // Create the Prisma client.
  createPrismaClient();
  
  // Apply any pending migrations.
  // Note that PGAdapter does not support the Prisma 'migrate' commands, other than 'migrate deploy'.
  await deployMigrations();
  
  await deleteExistingData();
  await createRandomSingersAndAlbumsAndTracks();
  await printSingersAndAlbums();
  await createVenueAndConcertInTransaction();
  await printAlbumsReleasedBefore1900();
  await updateVenueDescription();
  await staleRead();
  await createTicketSale();
  
  // Run 'npx prisma migrate deploy' once more, just to verify that we can run this command multiple
  // times on an existing database.
  await deployMigrations();

  if (pgAdapter) {
    await pgAdapter.stop();
  }
}

export async function startPGAdapter(): Promise<StartedTestContainer> {
  console.log("Pulling PGAdapter and Spanner emulator");
  const container: TestContainer = new GenericContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
    .withPullPolicy(PullPolicy.alwaysPull())
    .withExposedPorts(5432);
  console.log("Starting PGAdapter and Spanner emulator");
  return await container.start();
}
