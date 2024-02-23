"use strict";
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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const init_1 = require("./init");
const models_1 = require("../models/models");
const sequelize_1 = require("sequelize");
const crypto_1 = require("crypto");
const random_1 = require("./random");
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        // Start PGAdapter in a Docker test container.
        // PGAdapter will by default connect to the Spanner emulator.
        // The emulator runs in the same Docker container as PGAdapter.
        const pgAdapter = yield (0, init_1.startPGAdapter)();
        // Connect Sequelize to PGAdapter using the standard PostgreSQL Sequelize provider.
        const sequelize = new sequelize_1.Sequelize('sample-database', null, null, {
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
        yield (0, init_1.createDataModel)(sequelize);
        // Initialize the Sequelize models.
        (0, models_1.initModels)(sequelize);
        // Delete any existing test data in the database before running the sample.
        yield deleteAllData();
        // Create and then print some random data.
        yield createRandomSingersAndAlbums(sequelize, 20);
        yield printSingersAlbums();
        yield createVenuesAndConcerts(sequelize);
        // Close the sequelize connection pool and shut down PGAdapter.
        yield sequelize.close();
        yield pgAdapter.stop();
    });
}
function deleteAllData() {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Deleting all existing test data...");
        yield models_1.TicketSale.destroy({ truncate: true });
        yield models_1.Concert.destroy({ truncate: true });
        yield models_1.Venue.destroy({ truncate: true });
        yield models_1.Track.destroy({ truncate: true });
        yield models_1.Album.destroy({ truncate: true });
        yield models_1.Singer.destroy({ truncate: true });
        console.log("Finished deleting all existing test data");
    });
}
function createRandomSingersAndAlbums(sequelize, numSingers) {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Creating random singers and albums...");
        yield sequelize.transaction((tx) => __awaiter(this, void 0, void 0, function* () {
            // Generate some random singers.
            for (let i = 0; i < numSingers; i++) {
                const singer = yield models_1.Singer.create({
                    firstName: (0, random_1.randomFirstName)(),
                    lastName: (0, random_1.randomLastName)(),
                    active: Math.random() < 0.5
                }, { transaction: tx });
                // Generate some random albums.
                const numAlbums = (0, crypto_1.randomInt)(2, 10);
                const albums = new Array(numAlbums);
                for (let j = 0; j < numAlbums; j++) {
                    albums[j] = {
                        title: (0, random_1.randomAlbumTitle)(),
                        marketingBudget: Math.random() * 1000000,
                        SingerId: singer.id,
                    };
                }
                // Note that we assign the inserted albums to a new array, so we can use that when
                // inserting the tracks for these albums. The reason for this is that we need to
                // get the ID of the albums, and those are automatically assigned by the database
                // and returned by the `bulkCreate` function.
                const insertedAlbums = yield models_1.Album.bulkCreate(albums, { transaction: tx });
                // Generate some random tracks.
                for (const album of insertedAlbums) {
                    const numTracks = (0, crypto_1.randomInt)(5, 12);
                    const tracks = new Array(numTracks);
                    for (let k = 0; k < numTracks; k++) {
                        tracks[k] = {
                            id: album.id,
                            trackNumber: k + 1,
                            title: (0, random_1.randomTrackTitle)(),
                            sampleRate: Math.random(),
                        };
                    }
                    yield models_1.Track.bulkCreate(tracks, { transaction: tx });
                }
            }
        }));
        console.log("Finished creating singers and albums");
    });
}
function printSingersAlbums() {
    return __awaiter(this, void 0, void 0, function* () {
        const singers = yield models_1.Singer.findAll();
        for (const singer of singers) {
            console.log(`Singer ${singer.fullName} has albums:`);
            const albums = yield singer.getAlbums();
            for (const album of albums) {
                console.log(`\t${album.title}`);
            }
        }
    });
}
function createVenuesAndConcerts(sequelize) {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Creating venues and concerts...");
        yield sequelize.transaction((tx) => __awaiter(this, void 0, void 0, function* () {
            // Select a random singer.
            const singer = yield models_1.Singer.findOne({ limit: 1, transaction: tx });
            // Create a venue. Note that we can set the description as an object.
            // Description is mapped to a JSONB column.
            const venue = yield models_1.Venue.create({
                name: 'Avenue Park',
                description: '{Capacity: 5000, Location: "New York", Country: "US"}',
            }, { transaction: tx });
            // Create a concert and a ticket sale.
            const concert = yield models_1.Concert.create({
                name: 'Avenue Park Open',
                SingerId: singer.id,
                VenueId: venue.id,
                startTime: new Date('2023-02-01T20:00:00-05:00'),
                endTime: new Date('2023-02-02T02:00:00-05:00'),
            }, { transaction: tx });
            yield models_1.TicketSale.create({
                ConcertId: concert.id,
                customerName: `${(0, random_1.randomFirstName)()} ${(0, random_1.randomLastName)()}`,
                price: Math.random() * 1000,
                seats: ['A19', 'A20', 'A21'],
            }, { transaction: tx });
        }));
        console.log("Finished creating venues and concerts");
    });
}
(() => __awaiter(void 0, void 0, void 0, function* () {
    yield main();
}))().catch(e => {
    console.error(e);
});
