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

import {Knex} from "knex";
import {createDataModel, startPGAdapter} from './init';
import {Album, Concert, Singer, TicketSale, Track, Venue} from './model';
import {randomInt, randomUUID} from "crypto";
import {randomAlbumTitle, randomFirstName, randomLastName, randomTrackTitle} from "./random";

async function main() {
  // Start PGAdapter and the Spanner emulator in a Docker container.
  // Using a TestContainer to run PGAdapter is OK in development and test, but for production, it is
  // recommended to run PGAdapter as a side-car container.
  // See https://github.com/GoogleCloudPlatform/pgadapter/tree/postgresql-dialect/samples/cloud-run/nodejs
  // for a sample.
  const pgAdapter = await startPGAdapter();

  // Connect to PGAdapter with the standard PostgreSQL driver.
  const knex = require('knex')({
    client: 'pg',
    connection: {
      host: 'localhost',
      port: pgAdapter.getMappedPort(5432),
      database: 'knex-sample',
      ssl: false,
      jsonbSupport: true,
    }
  }) as Knex;

  // Create the sample tables (if they do not exist), and delete any existing test data before
  // running the sample.
  await createDataModel(knex);
  await deleteAllData(knex);

  // Create and then print some random data.
  await createRandomSingersAndAlbums(knex, 20);
  await printSingersAlbums(knex);

  // Create a Venue, Concert and TicketSale row.
  // The ticket_sales table uses an auto-generated primary key that is generated by a bit-reversed
  // sequence. The value can be returned to the application using a 'returning' clause.
  await createVenuesAndConcerts(knex);

  // Close the knex connection pool and shut down PGAdapter.
  await knex.destroy();
  await pgAdapter.stop();
}

async function createRandomSingersAndAlbums(knex: Knex, numSingers: number) {
  console.log("Creating random singers and albums...");
  const singers: Singer[] = new Array(numSingers);
  const albums: Album[] = [];
  const tracks: Track[] = [];

  await knex.transaction(async tx => {

    // Generate some random singers.
    for (let i=0; i<numSingers; i++) {
      singers[i] = {id: randomUUID(), first_name: randomFirstName(), last_name: randomLastName(),
        active: Math.random() < 0.5, created_at: new Date()} as Singer;

      // Generate some random albums.
      const numAlbums = randomInt(2, 10);
      for (let j=0; j<numAlbums; j++) {
        // Generate a random ID for the Album. This ID is also used for all the tracks of this
        // album, as the table "tracks" is interleaved in "albums".
        const album_id = randomUUID();
        albums.push({id: album_id, singer_id: singers[i].id, title: randomAlbumTitle(),
          marketing_budget: Math.random() * 1000000, created_at: new Date()} as Album);

        // Generate some random tracks.
        const numTracks = randomInt(5, 12);
        for (let k=0; k<numTracks; k++) {
          tracks.push({id: album_id, track_number: k+1, title: randomTrackTitle(),
            sample_rate: Math.random(), created_at: new Date()} as Track);
        }
      }
    }

    // Insert the data in batches of 50 elements.
    const batchSize = 50;
    for (let i=0; i<singers.length; i+=batchSize) {
      await tx.insert(singers.slice(i, i+batchSize)).into<Singer>('singers');
      process.stdout.write('.');
    }
    for (let i=0; i<albums.length; i+=batchSize) {
      await tx.insert(albums.slice(i, i+batchSize)).into<Album>('albums');
      process.stdout.write('.');
    }
    for (let i=0; i<tracks.length; i+=batchSize) {
      await tx.insert(tracks.slice(i, i+batchSize)).into<Track>('tracks');
      process.stdout.write('.');
    }
    console.log('');
  });
  console.log(`Finished creating ${singers.length} singers, ${albums.length} albums, and ${tracks.length} tracks.`);
}

async function printSingersAlbums(knex: Knex) {
  const singers = await knex.select('*').from<Singer>('singers').orderBy('last_name');
  for (const singer of singers) {
    console.log(`Singer ${singer.full_name} has albums:`);
    const albums = await knex.select('*')
        .from<Album>('albums')
        .where('singer_id', singer.id)
        .orderBy('title');
    for (const album of albums) {
      console.log(`\t${album.title}`);
    }
  }
}

async function createVenuesAndConcerts(knex: Knex) {
  console.log("Creating venues and concerts...");
  await knex.transaction(async tx => {
    const singer = await tx.select<Singer>('*').from('singers').first();
    const venue = {
      id: randomUUID(),
      name: 'Avenue Park',
      description: '{"Capacity": 5000, "Location": "New York", "Country": "US"}'
    } as Venue;
    // Use onConflict(...).merge() to execute an insert-or-update.
    await tx.insert<Venue>(venue).onConflict('id').merge().into('venues');
    const concert = {
      id: randomUUID(),
      name: 'Avenue Park Open',
      singer_id: singer!.id,
      venue_id: venue.id,
      start_time: new Date('2023-02-01T20:00:00-05:00'),
      end_time:   new Date('2023-02-02T02:00:00-05:00'),
    } as Concert;
    // Use onConflict(...).merge() to execute an insert-or-update.
    await tx.insert<Concert>(concert).onConflict('id').merge().into('concerts');

    // TicketSale uses an auto-generated primary key, so we don't need to supply a value for it.
    // The primary key value is generated by a bit-reversed sequence.
    const ticketSale = {
      concert_id: concert.id,
      customer_name: `${randomFirstName()} ${randomLastName()}`,
      price: Math.random() * 1000,
      seats: ['A19', 'A20', 'A21'],
    } as TicketSale;
    // The generated ID can be returned.
    const rows = await tx.insert<TicketSale>(ticketSale).into('ticket_sales').returning('id');
    ticketSale.id = rows[0].id;
  });
  console.log("Finished creating venues and concerts");
}

async function deleteAllData(knex: Knex) {
  console.log("Deleting all existing test data...");
  await knex<TicketSale>('ticket_sales').delete();
  await knex<Concert>('concerts').delete();
  await knex<Venue>('venues').delete();
  await knex<Track>('tracks').delete();
  await knex<Album>('albums').delete();
  await knex<Singer>('singers').delete();
  console.log("Finished deleting all existing test data");
}

(async () => {
  await main();
})().catch(e => {
  console.error(e);
  process.exit(1);
});
