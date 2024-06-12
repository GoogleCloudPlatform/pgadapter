# PGAdapter - Prisma Connection Options

PGAdapter has experimental support for the [Prisma](https://www.prisma.io/) version 4.8.1 and higher.

## Usage

First start PGAdapter:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -c /credentials.json \
  -x
```

Then connect to PGAdapter as if it was PostgreSQL. Include an additional connection option that
tells PGAdapter that you are using Prisma to connect. This will allow PGAdapter to apply specific
translations that are beneficial to Prisma:

```typescript
import { PrismaClient } from '@prisma/client'

process.env.DATABASE_URL = 'postgresql://localhost:5432/my-database?options=-c%20spanner.well_known_client=prisma';
const prisma = new PrismaClient();

helloWorld(prisma)
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });

async function helloWorld(prisma: PrismaClient) {
  const result = await prisma.$queryRaw`SELECT 'Hello world!' as hello`;
  console.log(result)
}
```

You can also connect to PGAdapter using Unix Domain Sockets if PGAdapter is running on the same host
as the client application, or the `/tmp` directory in the Docker container has been mapped to a
directory on the local machine:

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v /tmp:/tmp
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -c /credentials.json \
  -x
```

```typescript
import { PrismaClient } from '@prisma/client'

// Note: The connection string must contain a network host (e.g. 'localhost').
// This network host is ignored when a 'host' option is added to the connection string.
process.env.DATABASE_URL = 'postgresql://localhost:5432/my-database?host=/tmp&options=-c%20spanner.well_known_client=prisma';
const prisma = new PrismaClient();

helloWorld(prisma)
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });

async function helloWorld(prisma: PrismaClient) {
  const result = await prisma.$queryRaw`SELECT 'Hello world!' as hello`;
  console.log(result)
}
```


## Running PGAdapter

This example uses the pre-built Docker image to run PGAdapter.
See [README](../README.md) for more options for how to run PGAdapter.


## Performance Considerations

The following will give you the best possible performance when using node-postgres with PGAdapter.

### Unix Domain Sockets
Use Unix Domain Socket connections for the lowest possible latency when PGAdapter and the client
application are running on the same host. See https://node-postgres.com/features/connecting
for more information on connection options for node-postgres.

### Read-only Transactions
You can use [read-only transactions](https://cloud.google.com/spanner/docs/transactions#read-only_transactions)
with Prisma for Google Cloud Spanner. Using a read-only transaction instead of a read/write
transaction when your workload only executes read operations is more efficient.
This example shows how to execute a read-only transaction with Prisma:

```typescript
await prisma.$transaction(async tx => {
  await tx.$executeRaw`set transaction read only`;
  const user1 = await tx.user.findUnique({where: {id: "1"}})
  const user2 = await tx.user.findUnique({where: {id: "2"}})
});
```

### Stale reads
Cloud Spanner supports [executing stale reads](https://cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness).
Stale reads can have a performance advantage over strong reads, as Cloud Spanner can choose the
closest available replica to serve the read without blocking. The best way to execute stale reads
using Prisma is to create a separate Prisma client for serving stale reads:

```typescript
import { PrismaClient } from '@prisma/client'

process.env.DATABASE_URL = 'postgresql://localhost:5432/my-database';
const normalClient = new PrismaClient();

const staleReadClient = new PrismaClient({
  datasources: {
    db: {
      url: `postgresql://localhost:5432/my-database?options=-c spanner.read_only_staleness='MAX_STALENESS 10s' -c spanner.well_known_client=prisma'`,
    },
  },
});
```

## Limitations
- [Prisma Migrations](https://www.prisma.io/migrate) use a shadow database. This shadow database must
  be a real PostgreSQL database, and not a Cloud Spanner database. See https://www.prisma.io/docs/concepts/components/prisma-migrate/shadow-database#cloud-hosted-shadow-databases-must-be-created-manually
  for more information.
- Prisma Migrations rely heavily on `pg_catalog` tables and functions. Not all of these are supported
  in Cloud Spanner. Some migration operations are likely to not work or require manual intervention.
- Server-side auto-generated primary keys are not supported by Cloud Spanner. Use a client-side
  generated random UUID instead.
- [Upsert operations](https://www.prisma.io/docs/reference/api-reference/prisma-client-reference#upsert)
  are not supported by Cloud Spanner.
- `Prisma` often uses untyped query parameters encoded as strings. Cloud Spanner requires
  query parameters to be typed. PGAdapter therefore infers the types of the query parameters the
  first time it sees a given SQL string by analyzing the SQL statement and caching the result on the
  connection. This means that the first execution of a SQL statement with query parameters on a
  connection will be slightly slower than the following executions.
