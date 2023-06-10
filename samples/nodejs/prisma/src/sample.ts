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
import {
  randomAlbumTitle, randomElement,
  randomFirstName,
  randomId,
  randomLastName,
  randomReleaseDate, randomTrackTitle,
  times
} from "./random_names";
import {randomBytes, randomInt} from "crypto";

// This is the 'normal' Prisma client that can be used for strong reads and read/write transactions.
export const prisma = new PrismaClient();

// This client is read-only and will use a staleness of up to 10 seconds when reading data.
// These reads can have a slightly lower latency, but could return data that is up to 10 seconds
// stale.
export const staleReadClient = new PrismaClient({
  datasources: {
    db: {
      url: `${process.env.DATABASE_URL}?options=-c spanner.read_only_staleness='MAX_STALENESS 10s'`,
    },
  },
});

export async function deleteExistingData() {
  console.log("Deleting all existing concerts");
  let result = await prisma.concert.deleteMany({});
  console.log(`Deleted ${result.count} concerts`);

  console.log("Deleting all existing albums and related tracks");
  result = await prisma.album.deleteMany({});
  console.log(`Deleted ${result.count} albums`);

  console.log("Deleting all existing singers");
  result = await prisma.singer.deleteMany({});
  console.log(`Deleted ${result.count} singers`);

  console.log("Deleting all existing venues");
  result = await prisma.venue.deleteMany({});
  console.log(`Deleted ${result.count} venues`);

  console.log();
}

export async function createRandomSingersAndAlbumsAndTracks(numSingers?: number | undefined, numAlbums?: number | undefined, numTracks?: number | undefined) {
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
  const allAlbums: Prisma.AlbumCreateManyInput[] = [];
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
    allAlbums.push(...albums);
  }

  let numCreatedTracks = 0;
  for (const album of allAlbums) {
    const tracks: Prisma.TrackCreateManyInput[] = [];
    let n = 0;
    times(numTracks ?? randomInt(5, 15), () => tracks.push({
      id: album.id,
      trackNumber: ++n,
      title: randomTrackTitle(),
      sampleRate: Math.random(),
    }));
    const createdTracks = await prisma.track.createMany({data: tracks});
    console.log(`Created ${createdTracks.count} tracks for ${album.title}`);
    numCreatedTracks += createdTracks.count;
  }

  console.log(`Created ${singers.length} singers,  ${numCreatedAlbums} albums, and ${numCreatedTracks} tracks.`);
  console.log();
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
  console.log();
}

export async function createVenueAndConcertInTransaction() {
  console.log("Creating a Venue and a Concert in a transaction");
  const result = await prisma.$transaction(async (tx) => {
    // Get a random singer. Note that we read using the transaction.
    const singer = await tx.singer.findFirst({});

    // Create and save a Venue and a Concert for this singer.
    const venue = await tx.venue.create({
      data: {
        id: randomId(),
        name: "Avenue Park",
        description: {Capacity: 5000, Location: "New York", Country: "US"},
      }
    });
    const concert = await tx.concert.create({
      data: {
        id: randomId(),
        name: "Avenue Park Open",
        venueId: venue.id,
        singerId: singer.id,
        startTime: "2023-02-01T20:00:00-05:00",
        endTime: "2023-02-02T02:00:00-05:00",
      }
    });
    return {concert: concert, singer: singer, venue: venue};
  })
  console.log(`Created a concert for ${result.singer.fullName}`);
  console.log(`The concert is named ${result.concert.name} and start at ${result.concert.startTime} at ${result.venue.name}`);
  console.log();
}

export async function printAlbumsReleasedBefore1900() {
  console.log("Searching for albums released before 1900");
  const albums = await prisma.album.findMany({
    select: {
      title: true,
      releaseDate: true,
      singer: {
        select: {
          fullName: true,
        }
      },
    },
    where: {
      releaseDate: {
        lt: new Date('1900-01-01'),
      }
    },
    orderBy: {
      title: 'asc',
    },
  });
  for (const album of albums) {
    console.log(`Singer ${album.singer.fullName} released "${album.title}" on ${album.releaseDate}`);
  }
  console.log();
}

export async function updateVenueDescription() {
  console.log("Updating the description of venue 'Avenue Park");
  const updated = await prisma.$transaction(async (tx) => {
    const venue = await tx.venue.findFirst({
      where: {
        name: "Avenue Park",
      }
    });
    return tx.venue.update({
      data: {
        description: {Capacity: 10000, Location: "New York", Country: "US", Type: "Park"},
      },
      where: {
        id: venue.id,
      },
      select: {
        description: true
      }
    });
  })
  console.log("Updated description of venue 'Avenue Park'");
  console.log(`New description: ${JSON.stringify(updated.description)}`);
  console.log();
}

/** Executes a stale read on the Venue table. */
export async function staleRead() {
  console.log("Executing a stale read");
  // Read using the client that has been configured to use a 10-second max staleness.
  // This could mean that we are not seeing the most recent update to the table.
  const venue = await staleReadClient.venue.findFirst({
    where: {
      name: "Avenue Park",
    }
  });
  console.log(`Stale read returned a venue with description: ${JSON.stringify(venue.description)}`);
  console.log();
}
