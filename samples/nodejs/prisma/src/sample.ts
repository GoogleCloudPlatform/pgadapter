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

import {Prisma, PrismaClient} from "@prisma/client";
import * as fs from "fs";
import {
  randomAlbumTitle, randomElement,
  randomFirstName,
  randomId,
  randomLastName,
  randomReleaseDate,
  times
} from "./random_names";
import {randomBytes, randomInt} from "crypto";

export const prisma = new PrismaClient();
export const staleReadClient = new PrismaClient({
  datasources: {
    db: {
      url: `${process.env.DATABASE_URL}?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'`,
    },
  },
});

export async function createDataModel() {
  console.log("Creating sample data model...");
  const sqlStatements = fs
  .readFileSync('./prisma/create_data_model.sql')
  .toString()
  .split(";")
  .filter((sql) => sql.trim() !== '');
  for (const sql of sqlStatements) {
    await prisma.$executeRawUnsafe(sql);
  }
  console.log("Sample data model created.");
}

export async function createRandomSingersAndAlbums(numSingers?: number | undefined, numAlbums?: number | undefined) {
  console.log("Creating random singers and albums");
  const singers: Prisma.SingerCreateManyInput[] = [];
  times(numSingers ?? randomInt(5, 15), () => singers.push({
    id: randomId(),
    firstName: randomFirstName(),
    lastName: randomLastName(),
    active: true,
  }));
  const createdSingers = await prisma.singer.createMany({data: singers});
  console.log(`Created ${createdSingers.count} singers`);

  let numCreatedAlbums = 0;
  for (const singer of singers) {
    const albums: Prisma.AlbumCreateManyInput[] = [];
    times(numAlbums ?? randomInt(5, 15), () => albums.push({
      id: randomId(),
      title: randomAlbumTitle(),
      releaseDate: randomReleaseDate(),
      marketingBudget: Math.random() * 1000000,
      coverPicture: randomBytes(randomInt(2048) + 32),
      singerId: singer.id,
    }));
    const createdAlbums = await prisma.album.createMany({data: albums});
    console.log(`Created ${createdAlbums.count} albums for ${singer.firstName} ${singer.lastName}`);
    numCreatedAlbums += createdAlbums.count;
  }

  console.log(`Created ${singers.length} singers and ${numCreatedAlbums} albums.`);
}

export async function printSingersAndAlbums() {
  console.log("Printing all singers and albums");
  const singersAndAlbums = await prisma.singer.findMany({
    select: {
      fullName: true,
      albums: {
        select: {
          title: true,
          releaseDate: true,
        },
        orderBy: {
          title: 'asc',
        },
        take: 50000,
      },
    },
    orderBy: {
      lastName: 'asc',
    },
    take: 50000,
  });
  for (const singer of singersAndAlbums) {
    console.log(`Singer ${singer.fullName} has ${singer.albums.length} albums:`);
    for (const album of singer.albums) {
      console.log(`  ${album.title} was released on ${album.releaseDate}`);
    }
  }
}