import { pgTable, varchar, bigint, boolean, doublePrecision, integer, timestamp, date, customType, text } from 'drizzle-orm/pg-core';

// Custom type for bytea (Buffer in pg/node-postgres)
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

// Custom type for numeric/decimal (pg returns numeric as string)
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

// Custom type for jsonb (pg returns jsonb as object)
const jsonb = customType<{ data: any; driverData: string }>({
  dataType() {
    return 'jsonb';
  },
  toDriver(value: any): string {
    return JSON.stringify(value);
  },
  fromDriver(value: unknown): any {
    if (typeof value === 'string') {
      return JSON.parse(value);
    }
    return value;
  }
});

export const users = pgTable('users', {
  name: varchar('name', { length: 255 }).primaryKey(),
});

export const allTypes = pgTable('alltypes', {
  col_bigint: bigint('col_bigint', { mode: 'bigint' }).primaryKey(),
  col_bool: boolean('col_bool'),
  col_bytea: bytea('col_bytea'),
  col_float4: doublePrecision('col_float4'), // PGAdapter uses float4/float8, we'll map both to doublePrecision or real
  col_float8: doublePrecision('col_float8'),
  col_int: bigint('col_int', { mode: 'bigint' }), // Cloud Spanner INT64 is 8-byte
  col_numeric: numeric('col_numeric'),
  col_timestamptz: timestamp('col_timestamptz', { withTimezone: true }),
  col_date: date('col_date'),
  col_varchar: varchar('col_varchar', { length: 100 }),
  col_jsonb: jsonb('col_jsonb'),
});

import { relations } from 'drizzle-orm';

export const posts = pgTable('posts', {
  id: bigint('id', { mode: 'number' }).primaryKey(),
  title: varchar('title', { length: 255 }).notNull(),
  user_name: varchar('user_name', { length: 255 }).notNull().references(() => users.name),
});

export const usersRelations = relations(users, ({ many }) => ({
  posts: many(posts),
}));

export const postsRelations = relations(posts, ({ one }) => ({
  user: one(users, {
    fields: [posts.user_name],
    references: [users.name],
  }),
}));
