// Copyright 2026 Google LLC
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

import { Client } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { sql, eq } from 'drizzle-orm';
import { startPGAdapter, createDataModel } from './init';
import * as schema from './schema';
import { singers, albums, tracks, venues, concerts, ticketSales } from './schema';
import { randomUUID } from "crypto";
import { randomFirstName, randomLastName, randomAlbumTitle, randomTrackTitle, randomInt } from "./random";

async function main() {
  // Check if we should automatically start PGAdapter and Emulator in a container.
  const autoStart = process.env.AUTO_START_PGADAPTER?.toLowerCase() === 'true'
    || !process.env.DATABASE_URL;

  let pgAdapterContainer = null;
  let connectionString = process.env.DATABASE_URL;

  if (autoStart) {
    pgAdapterContainer = await startPGAdapter();
    const port = pgAdapterContainer.getMappedPort(5432);
    console.log(`PGAdapter and Spanner Emulator started on port ${port}`);
    connectionString = `postgresql://localhost:${port}/drizzle-sample`;
  }

  if (!connectionString) {
    throw new Error("No database connection string defined.");
  }

  // Connect using node-postgres Client
  const client = new Client({
    connectionString,
  });
  await client.connect();

  const db = drizzle(client, { schema });

  // Initialize database schema (DDL)
  await createDataModel(db);

  // Run database clean up
  await deleteAllData(db);

  // 1. Create Singers and Albums in a Read/Write transaction
  await createRandomSingersAndAlbums(db, 5);

  // 2. Query and print data
  await printSingersAlbums(db);

  // 3. Create Venue, Concert, and TicketSale (testing serial primary key generation)
  await createVenueConcertAndTicket(db);

  // 4. Perform a Stale Read
  await runStaleRead(db);

  // 5. Demonstrate Drizzle Relational Queries (requires real Spanner database)
  await runRelationalQueriesDemo(db);

  // Clean up connection
  await client.end();

  if (pgAdapterContainer) {
    console.log("Stopping PGAdapter container...");
    await pgAdapterContainer.stop();
  }
}

async function createRandomSingersAndAlbums(db: any, count: number) {
  console.log(`Creating ${count} random singers and their albums inside a transaction...`);

  await db.transaction(async (tx: any) => {
    for (let i = 0; i < count; i++) {
      const singerId = randomUUID();
      const firstName = randomFirstName();
      const lastName = randomLastName();

      // Insert Singer
      await tx.insert(singers).values({
        id: singerId,
        first_name: firstName,
        last_name: lastName,
        active: Math.random() < 0.5,
        created_at: new Date(),
        updated_at: new Date()
      });

      // Insert 2 random albums for this singer
      for (let j = 0; j < 2; j++) {
        const albumId = randomUUID();
        await tx.insert(albums).values({
          id: albumId,
          title: randomAlbumTitle(),
          singer_id: singerId,
          marketing_budget: String(randomInt(10000, 100000)),
          release_date: '2020-01-01',
          created_at: new Date(),
          updated_at: new Date()
        });

        // Insert some tracks
        for (let k = 1; k <= 3; k++) {
          await tx.insert(tracks).values({
            id: albumId,
            track_number: k,
            title: randomTrackTitle(),
            sample_rate: 44.1,
            created_at: new Date(),
            updated_at: new Date()
          });
        }
      }
    }
  });

  console.log("Transaction successfully committed.");
}

async function printSingersAlbums(db: any) {
  console.log("Printing all singers and their albums...");
  
  const results = await db.select().from(singers);
  for (const singer of results) {
    const name = singer.full_name || `${singer.first_name} ${singer.last_name}`;
    console.log(`Singer: ${name} (ID: ${singer.id})`);
    
    // Find albums for this singer
    const singerAlbums = await db.select().from(albums).where(eq(albums.singer_id, singer.id));
    for (const album of singerAlbums) {
      console.log(`  Album: "${album.title}"`);
    }
  }
}

async function createVenueConcertAndTicket(db: any) {
  console.log("Creating venue, concert, and ticket sale...");

  await db.transaction(async (tx: any) => {
    // 1. Get first singer
    const singerList = await tx.select().from(singers).limit(1);
    const singer = singerList[0];

    // 2. Insert Venue
    const venueId = randomUUID();
    await tx.insert(venues).values({
      id: venueId,
      name: "Acoustic Arena",
      description: "A lovely small acoustic venue",
      created_at: new Date(),
      updated_at: new Date()
    });

    // 3. Insert Concert
    const concertId = randomUUID();
    await tx.insert(concerts).values({
      id: concertId,
      name: "Live Unplugged",
      venue_id: venueId,
      singer_id: singer.id,
      start_time: new Date(),
      end_time: new Date(Date.now() + 3 * 3600000), // + 3 hours
      created_at: new Date(),
      updated_at: new Date()
    });

    // 4. Insert Ticket Sale (serial primary key will be generated and returned)
    const returnedIds = await tx.insert(ticketSales).values({
      concert_id: concertId,
      customer_name: "John Doe",
      price: "79.99",
      seats: ["Row A Seat 12", "Row A Seat 13"],
      created_at: new Date(),
      updated_at: new Date()
    }).returning({ id: ticketSales.id });

    console.log(`Successfully sold ticket. Generated serial ID returned: ${returnedIds[0].id}`);
  });
}

async function runStaleRead(db: any) {
  console.log("Executing a stale read (stale reads allow reading data with bounded staleness)...");
  
  // Set session staleness parameter for PGAdapter
  await db.execute(sql`SET spanner.read_only_staleness = 'MAX_STALENESS 10s'`);
  
  // Perform query under stale read
  const results = await db.select().from(singers).limit(1);
  if (results.length > 0) {
    console.log(`Stale read returned singer: ${results[0].last_name}`);
  }

  // Reset to default
  await db.execute(sql`RESET spanner.read_only_staleness`);
}

async function runRelationalQueriesDemo(db: any) {
  console.log("Demonstrating Drizzle Relational Queries (db.query)...");
  try {
    const results = await db.query.singers.findMany({
      with: {
        albums: true,
      },
      limit: 2,
    });
    console.log("Relational query successfully completed!");
    console.log(JSON.stringify(results, null, 2));
  } catch (err: any) {
    const isLateralJoinError = (err.message && err.message.includes("Lateral subqueries are not supported"))
      || (err.cause && err.cause.message && err.cause.message.includes("Lateral subqueries are not supported"));
    if (isLateralJoinError) {
      console.log("⚠️ Note: Drizzle Relational Queries (db.query) are not supported on Cloud Spanner because Spanner does not support lateral joins.");
    } else {
      console.error("Failed to run relational query demo:", err);
    }
  }
}

async function deleteAllData(db: any) {
  console.log("Cleaning up database test data...");
  await db.execute(sql`truncate table ticket_sales CASCADE`);
  await db.execute(sql`truncate table concerts CASCADE`);
  await db.execute(sql`truncate table venues CASCADE`);
  await db.execute(sql`truncate table tracks CASCADE`);
  await db.execute(sql`truncate table albums CASCADE`);
  await db.execute(sql`truncate table singers CASCADE`);
  console.log("Cleanup finished.");
}

main().catch(err => {
  console.error("An error occurred in sample application:", err);
  process.exit(1);
});
