# PGAdapter - Drizzle ORM Connection Options

PGAdapter supports [Drizzle ORM](https://orm.drizzle.team/) version `0.45.0` and higher, connecting via the standard `node-postgres` (`pg`) driver.

---

## 🚀 Usage

### Step 1: Start PGAdapter

Start PGAdapter using the pre-built Docker image:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -x
```

### Step 2: Configure Schema

Create a schema file (e.g. `schema.ts`) defining your database mapping:

```typescript
import { pgTable, varchar, bigint, boolean, timestamp } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: varchar('id', { length: 255 }).primaryKey(),
  first_name: varchar('first_name', { length: 255 }),
  last_name: varchar('last_name', { length: 255 }).notNull(),
  active: boolean('active'),
  created_at: timestamp('created_at', { withTimezone: true }),
});
```

### Step 3: Connect and Query

Initialize the database connection and perform queries:

```typescript
import { Client } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { users } from './schema';

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'my-database',
});
await client.connect();

const db = drizzle(client);

// Insert data
await db.insert(users).values({
  id: 'user-1',
  first_name: 'John',
  last_name: 'Doe',
  active: true,
  created_at: new Date()
});

// Select data
const results = await db.select().from(users);
console.log(results);

await client.end();
```

---

## ⚡ Transactions

Drizzle transactions map naturally to PostgreSQL transactions. PGAdapter supports standard Drizzle transactions:

```typescript
await db.transaction(async (tx) => {
  await tx.insert(users).values({
    id: 'user-2',
    first_name: 'Jane',
    last_name: 'Smith'
  });
  
  // Custom nested logic
});
```

---

## 💎 Spanner-Specific Features

### Stale Reads

Cloud Spanner supports low-latency [Stale Reads](https://cloud.google.com/spanner/docs/reads#read_with_timestamp_bound) for read-only operations. You can configure stale reads session-wide using PGAdapter session parameters:

```typescript
import { sql } from 'drizzle-orm';

// Enable stale reads with a maximum staleness of 10 seconds
await db.execute(sql`SET spanner.read_only_staleness = 'MAX_STALENESS 10s'`);

// All subsequent SELECT queries on this connection will execute as stale reads
const results = await db.select().from(users);

// Reset back to standard strong reads
await db.execute(sql`RESET spanner.read_only_staleness`);
```

---

## ⚡ Spanner Optimizations

### 1. DDL Batching
When executing schema generation or alterations, wrap your queries inside a single `START BATCH DDL` and `RUN BATCH` statement block to execute them in one Spanner DDL transaction:
```typescript
await db.execute(sql`
  START BATCH DDL;
  CREATE TABLE singers (...);
  CREATE TABLE albums (...);
  RUN BATCH;
`);
```

### 2. Batch DML
Execute multiple insert or update statements in a single batch transaction to reduce round-trips:
```typescript
await db.transaction(async (tx) => {
  await tx.execute(sql`START BATCH DML`);
  await tx.insert(users).values({ name: 'batch-foo' });
  await tx.insert(users).values({ name: 'batch-bar' });
  await tx.execute(sql`RUN BATCH`);
});
```

---

## ⚠️ Limitations & Best Practices

1. **Auto-increment columns**: Cloud Spanner recommends using **bit-reversed sequences** rather than standard monotonic auto-increment sequences to prevent write hotspots.
   Run the following on your database before generating sequence-based serial values:
   ```sql
   ALTER DATABASE db SET spanner.default_sequence_kind='bit_reversed_positive';
   ```
2. **Schema Migrations**: Automatic schema migrations using `drizzle-kit push` or `drizzle-kit generate` are not supported out-of-the-box. Cloud Spanner's PostgreSQL dialect supports a subset of standard PostgreSQL DDL statements. `drizzle-kit` may generate DDL statements (such as unsupported alters, constraints, or index definitions) that fail on Spanner. We recommend applying schema changes manually or curating the generated SQL migrations before execution.
3. **Drizzle Relational Queries (`db.query`)**: The Drizzle Relational Queries API (e.g. `db.query.users.findMany({ with: { posts: true } })`) is **not supported on Cloud Spanner** (both the service and the emulator) because Spanner does not support `LATERAL` subqueries (e.g. `LEFT JOIN LATERAL`), which Drizzle generates under the hood. Developers should instead use standard join queries or separate select queries when querying relationships.
