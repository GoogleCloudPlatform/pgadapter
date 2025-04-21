# PGAdapter and Prisma

PGAdapter has experimental support for [Prisma](https://prisma.io/). This document shows
how to use this sample application, and lists the limitations when working with `Prisma` with PGAdapter.

The [src/sample.ts](src/sample.ts) file contains a sample application using `Prisma` with PGAdapter.
Use this as a reference for features of `Prisma` that are supported with PGAdapter.
This sample assumes that the reader is familiar with `Prisma`, and
it is not intended as a tutorial for how to use `Prisma` in general.

__NOTE__: In order for all features that are mentioned in this document to work, it is important
that your connection string contains the following addition: `?options=-c%20spanner.well_known_client=prisma`.
That will ensure that PGAdapter recognizes the connecting client as Prisma. This ensures that
PGAdapter can apply specific query translations for Prisma.
See [this environment file](.env) for a full example.


## Experimental Support
Experimental Support means that `Prisma` can be used with Cloud Spanner PostgreSQL databases, but
with limitations. Applications that have been developed with `Prisma` for PostgreSQL will require
modifications before they can be used with Cloud Spanner PostgreSQL databases. It is possible to
develop new applications using `Prisma` with Cloud Spanner PostgreSQL databases, as long as the
limitations are taken into account. These applications will also work with PostgreSQL without
modifications.

See [Limitations](#limitations) for a full list of limitations when working with `Prisma`.


# Running the Sample Application

The sample application is set up to automatically do the following:
1. Start PGAdapter and the Spanner Emulator in a Docker container.
2. Dynamically set the `DATABASE_URL` environment variable to point to the PGAdapter + Emulator
   instance that was started by the sample script.
3. Create the Spanner instance and database on the Spanner Emulator.
4. Run `npx prisma migrate deploy` on the database to create the sample application tables.
5. Run the sample application. This will create, query, update, and delete some sample data.
6. Run `npx prisma migrate deploy` once more on the database to show that running this multiple
   times is supported.

Run the sample application with the following command:

```shell
npm run start
```

# Manually Configuring and Running the Sample

The sample application automatically starts PGAdapter and the Spanner Emulator when the application
is run. You can also manually start PGAdapter and configure it to connect to a real Spanner
instance.

## Start PGAdapter
You must start PGAdapter before you can run the sample. The following command shows how to start PGAdapter using the
pre-built Docker image. See [Running PGAdapter](../../../README.md#usage) for more information on other options for how
to run PGAdapter.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  -d -p 5432:5432 \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:/credentials.json:ro \
  -v /tmp:/tmp \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -p my-project -i my-instance \
  -c /credentials.json \
  -x
```

## Configuration

Modify the .env file in this directory, so it corresponds to your local setup:
1. Modify the `DATABASE_URL` variable to point to your PGAdapter
   instance and your database. Do not modify or remove the
   `?sslmode=disable&options=-c%20spanner.well_known_client=prisma` section.
2. Set the `AUTO_START_PGADAPTER` variable to `false`.

## Create the Sample Data Model

Running the sample application will automatically create the database schema. You can also create
the database schema manually with the following command:

```shell
npx prisma migrate deploy
```

__NOTE__: Spanner PostgreSQL and PGAdapter do not support any other Prisma `migrate` commands than
`prisma migrate deploy`.

## Data Types

See the [prisma/schema.prisma](prisma/schema.prisma) file for an example mapping for all data types.

Spanner supports the following data types in combination with `Prisma`.

| PostgreSQL Type                        | Prisma type       |
|----------------------------------------|-------------------|
| boolean                                | Boolean           |
| bigint / int8                          | BigInt            |
| varchar                                | String            |
| text                                   | String            |
| float8 / double precision              | Float             |
| numeric                                | Decimal           |
| timestamptz / timestamp with time zone | DateTime          |
| date                                   | DateTime @db.Date |
| bytea                                  | Bytes             |
| jsonb                                  | Json              |

## Prisma Studio

[Prisma Studio](https://www.prisma.io/docs/orm/tools/prisma-studio) can be used with PGAdapter and
Spanner. Make sure to start PGAdapter first, and then let Prisma Studio connect to PGAdapter as if
it were PostgreSQL.

## Limitations
The following limitations are currently known:

| Limitation             | Workaround                                                                                                                                                                                                                                                                                                                              |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Migrations             | Cloud Spanner does not support the full PostgreSQL DDL dialect, and also not all `pg_catalog` tables and functions. PGAdapter contains translations for the most commonly used features that are not supported. It is possible to use Prisma Migrations with PGAdapter, but some migrations are likely to require manual modifications. |
| Shadow Database        | Cloud Spanner cannot be used as a shadow database. Instead, you need to set up a real PostgreSQL database and use that as the shadow database. See https://www.prisma.io/docs/concepts/components/prisma-migrate/shadow-database#cloud-hosted-shadow-databases-must-be-created-manually                                                 |
| Advisory locks         | Advisory locks are not supported. Prisma will try to use this during migrations. These commands will be ignored by PGAdapter.                                                                                                                                                                                                           |
| Generated primary keys | Auto-generated primary keys are supported, but do not generate a monotonically increasing value. The `@default(autoincrement())` annotation in combination with the `serial` data type will generate bit-reversed sequential values that are safe from causing hotspots in your database.                                               |
| Upsert                 | Upsert is not supported.                                                                                                                                                                                                                                                                                                                |

### Migrations
Prisma Migrations make extensive use of `pg_catalog` tables and functions. Not all of these are supported
by Spanner. Spanner and PGAdapter therefore do not support Prisma `migrate` commands other than
`prisma migrate deploy`.

An alternative way to use Prisma Migrations with Cloud Spanner PostgreSQL is to execute the
migration commands on an open-source PostgreSQL database. See [migrations.md](migrations.md) for
more information.

The DDL that is generated by a Prisma Migration command can be modified manually. This is sometimes
needed to add features that are not supported by Prisma, or if you want to add specific Cloud Spanner
features to your data model. This sample application for example creates an `INTERLEAVED` table.
It can also be necessary if Prisma generates DDL statements that are not supported by Cloud Spanner.

See also:
- https://www.prisma.io/docs/concepts/components/prisma-migrate/migrate-development-production#customizing-migrations
- https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#extensions_to

### Shadow Database
Prisma Migrations use a shadow database for generating and verifying new migrations. Spanner
cannot be used for this purpose. Instead, you need to set up a separate PostgreSQL database that can
be used as the shadow database.

See also https://www.prisma.io/docs/concepts/components/prisma-migrate/shadow-database#cloud-hosted-shadow-databases-must-be-created-manually.

You can also use an open-source PostgreSQL database to execute all Prisma migrations command. See
[migrations.md](migrations.md) for more information.

### Advisory Locks
Prisma Migrations will use [advisory locks during migrations](https://www.prisma.io/docs/concepts/components/prisma-migrate/migrate-development-production#advisory-locking).
Cloud Spanner and PGAdapter do not support advisory locks. The advisory lock statements will
therefore be ignored and have no effect.

### Generated Primary Keys
Generated primary keys are supported and use values that are not monotonically increasing. This
prevents hotspots in your database. See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more information. The `TicketSale` model in this sample uses an auto-generated primary key. The
primary key is generated by a column with type `serial`, which uses a backing bit-reversed sequence
to generate unique values.

```typescript
model TicketSale {
   id           Int @id @default(autoincrement())
   createdAt    DateTime @default(now())
   updatedAt    DateTime @updatedAt
   concert      Concert @relation(fields: [concertId], references: [id])
   concertId    String
   customerName String
   price        Decimal
   seats        String[]
}
```

### Generated Columns
Generated columns can be used, but these are not supported by Prisma Migrations. You need to manually
modify the generated migration script to create a generated column. This sample uses a generated
column for the `fullName` property of `Singer`. See [migration.sql](prisma/migrations/20230608163206_init/migration.sql)
for an example.

### Upsert
`Upsert` statements are not supported by Spanner and should not be used.
