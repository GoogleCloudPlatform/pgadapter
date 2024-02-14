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

import {GenericContainer, PullPolicy, StartedTestContainer, TestContainer} from "testcontainers";
import {Knex} from "knex";

/**
 * Creates the data model that is needed for this sample application.
 *
 * The Cloud Spanner PostgreSQL dialect does not support all system tables (pg_catalog tables) that
 * are present in open-source PostgreSQL databases. Those tables are used by Sequelize migrations.
 * Migrations are therefore not supported.
 */
export async function createDataModel(knex: Knex) {
  console.log("Checking whether tables already exists");
  const result: any = await knex.raw(`
        SELECT COUNT(1) AS c 
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name IN ('singers', 'albums', 'tracks', 'venues', 'concerts', 'ticket_sales')`);
  if (result.rows[0].c == '6') {
    console.log("Sample data model already exists, not creating any new tables");
    return;
  }
  console.log("Creating tables...");
  // Create the data model.
  await knex.raw(
          `
              create table if not exists singers (
                 id         varchar not null primary key,
                 first_name varchar,
                 last_name  varchar not null,
                 full_name varchar(300) generated always as (
                                CASE WHEN first_name IS NULL THEN last_name
                                     WHEN last_name  IS NULL THEN first_name
                                     ELSE first_name || ' ' || last_name
                                    END) stored,
                 active     boolean,
                 created_at timestamptz,
                 updated_at timestamptz
              );

              create table if not exists albums (
                id               varchar not null primary key,
                title            varchar not null,
                marketing_budget numeric,
                release_date     date,
                cover_picture    bytea,
                singer_id        varchar not null,
                created_at       timestamptz,
                updated_at       timestamptz,
                constraint fk_albums_singers foreign key (singer_id) references singers (id)
              );

              create table if not exists tracks (
                id           varchar not null,
                track_number bigint not null,
                title        varchar not null,
                sample_rate  float8 not null,
                created_at   timestamptz,
                updated_at   timestamptz,
                primary key (id, track_number)
              ) interleave in parent albums on delete cascade;

              create table if not exists venues (
                id          varchar not null primary key,
                name        varchar not null,
                description varchar not null,
                created_at  timestamptz,
                updated_at  timestamptz
              );

              create table if not exists concerts (
                id          varchar not null primary key,
                venue_id    varchar not null,
                singer_id   varchar not null,
                name        varchar not null,
                start_time  timestamptz not null,
                end_time    timestamptz not null,
                created_at  timestamptz,
                updated_at  timestamptz,
                constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
                constraint fk_concerts_singers foreign key (singer_id) references singers (id),
                constraint chk_end_time_after_start_time check (end_time > start_time)
              );

              -- Create a bit-reversed sequence that will be used to generate identifiers for the ticket_sales table.
              -- See also https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#create_sequence
              -- Note that the 'bit_reversed_positive' keyword is required for Spanner,
              -- and is automatically skipped for open-source PostgreSQL.
              create sequence if not exists ticket_sale_seq
                bit_reversed_positive
                skip range 1 1000
                start counter with 50000;

              create table if not exists ticket_sales (
                id bigint not null primary key default nextval('ticket_sale_seq'),
                concert_id       varchar not null,
                customer_name    varchar not null,
                price            decimal not null,
                seats            text[],
                created_at       timestamptz,
                updated_at       timestamptz,
                constraint fk_ticket_sales_concerts foreign key (concert_id) references concerts (id)
              );
          `);
  console.log("Finished creating tables");
}

export async function startPGAdapter(): Promise<StartedTestContainer> {
  console.log("Pulling PGAdapter and Spanner emulator");
  const container: TestContainer = new GenericContainer("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
      .withPullPolicy(PullPolicy.alwaysPull())
      .withExposedPorts(5432);
  console.log("Starting PGAdapter and Spanner emulator");
  return await container.start();
}
