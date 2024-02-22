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
exports.shutdownSequelize = exports.createDataModel = exports.startSequelize = exports.sequelize = void 0;
const sequelize_1 = require("sequelize");
const testcontainers_1 = require("testcontainers");
let pgadapter;
function startSequelize() {
    return __awaiter(this, void 0, void 0, function* () {
        // Start PGAdapter in a Docker test container.
        // PGAdapter will by default connect to the Spanner emulator.
        // The emulator runs in the same Docker container as PGAdapter.
        pgadapter = yield startPGAdapter();
        console.log("Connecting...");
        exports.sequelize = new sequelize_1.Sequelize('sample-database', null, null, {
            dialect: "postgres",
            dialectOptions: {
                clientMinMessages: 'ignore',
            },
            // Connect to PGAdapter on localhost and the randomly assigned port that is mapped to port 5432
            // in the test container.
            host: 'localhost',
            port: pgadapter.getMappedPort(5432),
            // Setting the timezone is required.
            timezone: 'UTC',
            // The following configuration options are optional.
            omitNull: false,
            retry: { max: 5 },
            pool: {
                max: 50, min: 10, acquire: 2000, idle: 20000,
            },
            logging: false,
        });
        yield createDataModel();
    });
}
exports.startSequelize = startSequelize;
/**
 * Creates the data model that is needed for this sample application.
 *
 * The Cloud Spanner PostgreSQL dialect does not support all system tables (pg_catalog tables) that
 * are present in open-source PostgreSQL databases. Those tables are used by Sequelize migrations.
 * Migrations are therefore not supported.
 */
function createDataModel() {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Checking whether tables already exists");
        const result = yield exports.sequelize.query("SELECT COUNT(1) AS c FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('Singers', 'Albums')", { type: sequelize_1.QueryTypes.SELECT, raw: true, plain: true });
        if (result.c == '2') {
            return;
        }
        console.log("Creating tables");
        // Create the data model.
        yield exports.sequelize.query(`create sequence if not exists singers_seq bit_reversed_positive;
           create table "Singers" (
             id bigint not null primary key default nextval('singers_seq'),
             "firstName" varchar,
             "lastName" varchar,
             "fullName" varchar generated always as (
                CASE WHEN "firstName" IS NULL THEN "lastName"
                     WHEN "lastName"  IS NULL THEN "firstName"
                     ELSE "firstName" || ' ' || "lastName"
                END) stored,
             "active" boolean,
             "createdAt" timestamptz,
             "updatedAt" timestamptz
           );
           create sequence if not exists albums_seq bit_reversed_positive;
           create table "Albums" (
             id bigint not null primary key default nextval('albums_seq'),
             title varchar,
             "SingerId" bigint,
             "createdAt" timestamptz,
             "updatedAt" timestamptz,
             constraint fk_albums_singers foreign key ("SingerId") references "Singers" (id)
           )`, { type: sequelize_1.QueryTypes.RAW });
    });
}
exports.createDataModel = createDataModel;
function shutdownSequelize() {
    return __awaiter(this, void 0, void 0, function* () {
        if (exports.sequelize) {
            yield exports.sequelize.close();
        }
        if (pgadapter) {
            console.log("Stopping PGAdapter and Spanner emulator");
            yield pgadapter.stop();
            pgadapter = undefined;
        }
    });
}
exports.shutdownSequelize = shutdownSequelize;
function startPGAdapter() {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Pulling PGAdapter and Spanner emulator");
        const container = new testcontainers_1.GenericContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
            .withPullPolicy(testcontainers_1.PullPolicy.alwaysPull())
            .withExposedPorts(5432);
        console.log("Starting PGAdapter and Spanner emulator");
        return yield container.start();
    });
}
