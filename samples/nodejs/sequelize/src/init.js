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
exports.startPGAdapter = exports.createDataModel = void 0;
const sequelize_1 = require("sequelize");
const testcontainers_1 = require("testcontainers");
/**
 * Creates the data model that is needed for this sample application.
 *
 * The Cloud Spanner PostgreSQL dialect does not support all system tables (pg_catalog tables) that
 * are present in open-source PostgreSQL databases. Those tables are used by Sequelize migrations.
 * Migrations are therefore not supported.
 */
function createDataModel(sequelize) {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Checking whether tables already exists");
        const result = yield sequelize.query(`SELECT COUNT(1) AS c
           FROM information_schema.tables
           WHERE table_schema='public'
             AND table_name IN ('Singers', 'Albums', 'Tracks', 'Venues', 'Concerts', 'TicketSales')`, { type: sequelize_1.QueryTypes.SELECT, raw: true, plain: true });
        if (result.c == '6') {
            return;
        }
        console.log("Creating tables");
        // Create the data model.
        yield sequelize.query(`
            create sequence if not exists singers_seq bit_reversed_positive;
            create table "Singers" (
              id          bigint not null primary key default nextval('singers_seq'),
              "firstName" varchar,
              "lastName"  varchar,
              "fullName"  varchar generated always as (
                 CASE WHEN "firstName" IS NULL THEN "lastName"
                      WHEN "lastName"  IS NULL THEN "firstName"
                      ELSE "firstName" || ' ' || "lastName"
                 END) stored,
              "active"    boolean,
              "createdAt" timestamptz,
              "updatedAt" timestamptz
            );
            
            create sequence if not exists albums_seq bit_reversed_positive;
            create table "Albums" (
              id                bigint not null primary key default nextval('albums_seq'),
              title             varchar,
              "marketingBudget" numeric,
              "SingerId"        bigint,
              "createdAt"       timestamptz,
              "updatedAt"       timestamptz,
              constraint fk_albums_singers foreign key ("SingerId") references "Singers" (id)
            );

            create table if not exists "Tracks" (
              id            bigint not null,
              "trackNumber" bigint not null,
              title         varchar not null,
              "sampleRate"  float8 not null,
              "createdAt"   timestamptz,
              "updatedAt"   timestamptz,
              primary key (id, "trackNumber")
            ) interleave in parent "Albums" on delete cascade;

            create sequence if not exists venues_seq bit_reversed_positive;
            create table if not exists "Venues" (
              id          bigint not null primary key default nextval('venues_seq'),
              name        varchar not null,
              description varchar not null,
              "createdAt" timestamptz,
              "updatedAt" timestamptz
            );

            create sequence if not exists concerts_seq bit_reversed_positive;
            create table if not exists "Concerts" (
              id          bigint not null primary key default nextval('concerts_seq'),
              "VenueId"   bigint not null,
              "SingerId"  bigint not null,
              name        varchar not null,
              "startTime" timestamptz not null,
              "endTime"   timestamptz not null,
              "createdAt" timestamptz,
              "updatedAt" timestamptz,
              constraint fk_concerts_venues  foreign key ("VenueId")  references "Venues"  (id),
              constraint fk_concerts_singers foreign key ("SingerId") references "Singers" (id),
              constraint chk_end_time_after_start_time check ("endTime" > "startTime")
            );

            create sequence if not exists ticket_sales_seq bit_reversed_positive;
            create table if not exists "TicketSales" (
              id             bigint not null primary key default nextval('ticket_sales_seq'),
              "ConcertId"    bigint not null,
              "customerName" varchar not null,
              price          decimal not null,
              seats          text[],
              "createdAt"    timestamptz,
              "updatedAt"    timestamptz,
              constraint fk_ticket_sales_concerts foreign key ("ConcertId") references "Concerts" (id)
            );
           `, { type: sequelize_1.QueryTypes.RAW });
    });
}
exports.createDataModel = createDataModel;
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
exports.startPGAdapter = startPGAdapter;
