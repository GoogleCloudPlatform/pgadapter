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

import { pgTable, varchar, bigint, boolean, timestamp, date, customType, text, serial } from 'drizzle-orm/pg-core';

// Custom type for bytea (Buffer)
const bytea = customType<{ data: Buffer; driverData: string }>({
  dataType() {
    return 'bytea';
  },
  toDriver(value: Buffer): string {
    return '\\x' + value.toString('hex');
  },
  fromDriver(value: unknown): Buffer {
    if (typeof value === 'string') {
      return Buffer.from(value.substring(2), 'hex');
    }
    return value as Buffer;
  }
});

// Custom type for numeric/decimal
const numeric = customType<{ data: string; driverData: string }>({
  dataType() {
    return 'numeric';
  },
  toDriver(value: string): string {
    return value;
  },
  fromDriver(value: unknown): string {
    return String(value);
  }
});

export const singers = pgTable('singers', {
  id: varchar('id', { length: 255 }).primaryKey(),
  first_name: varchar('first_name', { length: 255 }),
  last_name: varchar('last_name', { length: 255 }).notNull(),
  full_name: varchar('full_name', { length: 300 }), // Generated column
  active: boolean('active'),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
});

export const albums = pgTable('albums', {
  id: varchar('id', { length: 255 }).primaryKey(),
  title: varchar('title', { length: 255 }).notNull(),
  marketing_budget: numeric('marketing_budget'),
  release_date: date('release_date'),
  cover_picture: bytea('cover_picture'),
  singer_id: varchar('singer_id', { length: 255 }).notNull().references(() => singers.id),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
});

export const tracks = pgTable('tracks', {
  id: varchar('id', { length: 255 }).notNull(),
  track_number: bigint('track_number', { mode: 'number' }).notNull(),
  title: varchar('title', { length: 255 }).notNull(),
  sample_rate: customType<{ data: number; driverData: number }>({
    dataType() {
      return 'float8';
    },
    toDriver(value: number) { return value; },
    fromDriver(value: unknown) { return Number(value); }
  })('sample_rate').notNull(),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
}, (table) => {
  return [
    // Multi-column primary key
    {
      pk: [table.id, table.track_number]
    }
  ];
});

export const venues = pgTable('venues', {
  id: varchar('id', { length: 255 }).primaryKey(),
  name: varchar('name', { length: 255 }).notNull(),
  description: varchar('description', { length: 1000 }).notNull(),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
});

export const concerts = pgTable('concerts', {
  id: varchar('id', { length: 255 }).primaryKey(),
  venue_id: varchar('venue_id', { length: 255 }).notNull().references(() => venues.id),
  singer_id: varchar('singer_id', { length: 255 }).notNull().references(() => singers.id),
  name: varchar('name', { length: 255 }).notNull(),
  start_time: timestamp('start_time', { withTimezone: true }).notNull(),
  end_time: timestamp('end_time', { withTimezone: true }).notNull(),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
});

export const ticketSales = pgTable('ticket_sales', {
  id: serial('id').primaryKey(),
  concert_id: varchar('concert_id', { length: 255 }).notNull().references(() => concerts.id),
  customer_name: varchar('customer_name', { length: 255 }).notNull(),
  price: numeric('price').notNull(),
  seats: customType<{ data: string[]; driverData: string[] }>({
    dataType() {
      return 'text[]';
    },
    toDriver(value: string[]): string[] { return value; },
    fromDriver(value: unknown): string[] {
      return value as string[];
    }
  })('seats'),
  created_at: timestamp('created_at', { withTimezone: true }),
  updated_at: timestamp('updated_at', { withTimezone: true }),
});

import { relations } from 'drizzle-orm';

export const singersRelations = relations(singers, ({ many }) => ({
  albums: many(albums),
}));

export const albumsRelations = relations(albums, ({ one }) => ({
  singer: one(singers, {
    fields: [albums.singer_id],
    references: [singers.id],
  }),
}));
