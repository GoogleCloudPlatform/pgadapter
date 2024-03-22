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

import {createDataModel, startPGAdapter} from './init'
import {Album, Concert, initModels, Singer, TicketSale, Track, Venue} from '../models/models';
import {Sequelize} from "sequelize";
import {randomInt} from "crypto";
import {randomAlbumTitle, randomFirstName, randomLastName, randomTrackTitle} from "./random";

async function main() {
  // Start PGAdapter in a Docker test container.
  // PGAdapter will by default connect to the Spanner emulator.
  // The emulator runs in the same Docker container as PGAdapter.
  const pgAdapter = await startPGAdapter();

  // Connect Sequelize to PGAdapter using the standard PostgreSQL Sequelize provider.
  const sequelize = new Sequelize('sample-database', null, null, {
    dialect: "postgres",

    // Connect to PGAdapter on localhost and the randomly assigned port that is mapped to port 5432
    // in the test container.
    host: 'localhost',
    port: pgAdapter.getMappedPort(5432),

    // Setting the timezone is required, as Sequelize otherwise tries to use an INTERVAL to set
    // the timezone. That is not supported on PGAdapter, and you will get the following error:
    // invalid value for parameter "TimeZone": "INTERVAL '+00:00' HOUR TO MINUTE"
    timezone: 'UTC',

    // The following configuration options are optional.
    omitNull: false,
    pool: {
      max: 50, min: 10, acquire: 2000, idle: 20000,
    },
    logging: false,
  });

  // Create the tables that are needed for this sample (if they do not already exist).
  await createDataModel(sequelize);

  // Initialize the Sequelize models.
  initModels(sequelize);

  // Delete any existing test data in the database before running the sample.
  await deleteAllData();

  // Create and then print some random data.
  await createRandomSingersAndAlbums(sequelize, 20);
  await printSingersAlbums();

  await createVenuesAndConcerts(sequelize);

  // Close the sequelize connection pool and shut down PGAdapter.
  await sequelize.close();
  await pgAdapter.stop();
}

async function deleteAllData() {
  console.log("Deleting all existing test data...");
  await TicketSale.destroy({truncate: true});
  await Concert.destroy({truncate: true});
  await Venue.destroy({truncate: true});
  await Track.destroy({truncate: true});
  await Album.destroy({truncate: true});
  await Singer.destroy({truncate: true});
  console.log("Finished deleting all existing test data");
}

async function createRandomSingersAndAlbums(sequelize: Sequelize, numSingers: number) {
  console.log("Creating random singers and albums...");
  await sequelize.transaction(async tx => {
    // Generate some random singers.
    for (let i=0; i<numSingers; i++) {
      const singer = await Singer.create({
        firstName: randomFirstName(),
        lastName: randomLastName(),
        active: Math.random() < 0.5
      }, {transaction: tx});

      // Generate some random albums.
      const numAlbums = randomInt(2, 10);
      const albums: Album[] = new Array(numAlbums);
      for (let j=0; j<numAlbums; j++) {
        albums[j] = {
          title: randomAlbumTitle(),
          marketingBudget: Math.random() * 1000000,
          SingerId: singer.id,
        } as Album;
      }
      // Note that we assign the inserted albums to a new array, so we can use that when
      // inserting the tracks for these albums. The reason for this is that we need to
      // get the ID of the albums, and those are automatically assigned by the database
      // and returned by the `bulkCreate` function.
      const insertedAlbums = await Album.bulkCreate(albums, {transaction: tx});

      // Generate some random tracks.
      for (const album of insertedAlbums) {
        const numTracks = randomInt(5, 12);
        const tracks: Track[] = new Array(numTracks);
        for (let k = 0; k < numTracks; k++) {
          tracks[k] = {
            id: album.id,
            trackNumber: k + 1,
            title: randomTrackTitle(),
            sampleRate: Math.random(),
          } as Track;
        }
        await Track.bulkCreate(tracks, {transaction: tx});
      }
    }
  });
  console.log("Finished creating singers and albums");
}

async function printSingersAlbums() {
  const singers = await Singer.findAll();
  for (const singer of singers) {
    console.log(`Singer ${singer.fullName} has albums:`);
    const albums = await singer.getAlbums();
    for (const album of albums) {
      console.log(`\t${album.title}`);
    }
  }
}

async function createVenuesAndConcerts(sequelize: Sequelize) {
  console.log("Creating venues and concerts...");
  await sequelize.transaction(async tx => {
    // Select a random singer.
    const singer = await Singer.findOne({limit: 1, transaction: tx});

    // Create a venue. Note that we can set the description as an object.
    // Description is mapped to a JSONB column.
    const venue = await Venue.create({
      name: 'Avenue Park',
      description: '{Capacity: 5000, Location: "New York", Country: "US"}',
    }, {transaction: tx});

    // Create a concert and a ticket sale.
    const concert = await Concert.create({
      name: 'Avenue Park Open',
      SingerId:  singer.id,
      VenueId:   venue.id,
      startTime: new Date('2023-02-01T20:00:00-05:00'),
      endTime:   new Date('2023-02-02T02:00:00-05:00'),
    }, {transaction: tx});

    await TicketSale.create({
      ConcertId:    concert.id,
      customerName: `${randomFirstName()} ${randomLastName()}`,
      price:        Math.random() * 1000,
      seats:        ['A19', 'A20', 'A21'],
    }, {transaction: tx});
  });
  console.log("Finished creating venues and concerts");
}

(async () => {
  await main();
})().catch(e => {
  console.error(e);
});
