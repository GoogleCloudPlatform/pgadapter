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

import {PrismaClient} from '@prisma/client'
import {
  createRandomSingersAndAlbumsAndTracks,
  createVenueAndConcertInTransaction,
  deleteExistingData, printAlbumsReleasedBefore1900,
  printSingersAndAlbums, staleRead, updateVenueDescription
} from "./sample";

export const prisma = new PrismaClient();
export const staleReadClient = new PrismaClient({
  datasources: {
    db: {
      // This environment variable is also defined in the .env file.
      // It sets an option in the connection string to instruct PGAdapter to use a maximum staleness
      // of 10 seconds for read operations.
      url: `${process.env.STALE_READ_DATABASE_URL}`,
    },
  },
});

console.log("Running sample application...");

runSample()
  .then(async () => {
    console.log("Successfully executed sample application");
    await prisma.$disconnect();
    await staleReadClient.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    await staleReadClient.$disconnect();
  });

async function runSample() {
  await deleteExistingData();
  await createRandomSingersAndAlbumsAndTracks();
  await printSingersAndAlbums();
  await createVenueAndConcertInTransaction();
  await printAlbumsReleasedBefore1900();
  await updateVenueDescription();
  await staleRead();
}
