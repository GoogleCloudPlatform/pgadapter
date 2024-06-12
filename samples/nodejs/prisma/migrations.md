# Prisma Migrations with PGAdapter

Prisma migration commands, other than `prisma migration deploy`, are not supported on Cloud Spanner
PostgreSQL, as they use `pg_catalog` tables and other features of PostgreSQL that are not supported
by Cloud Spanner.

You can however use a local PostgreSQL database and execute Prisma migration commands on that
database instead.

## Empty PostgreSQL Database

1. Create an empty PostgreSQL database, either on a local server or in a Docker container.
2. Modify the `.env` file to point to this local database instead of Cloud Spanner.
3. Execute `npx prisma migrate deploy` to deploy all your migrations to this database.
4. Make any changes to your Prisma code and execute `prisma migrate dev --name my_migration_name` to
   generate the migration.
5. Save the migration script to your code repository. Make any manual changes that might be needed,
   for example if Prisma generated DDL statements that are not supported by Cloud Spanner. It is
   recommended to try the DDL statements manually on a Cloud Spanner database before committing them 
   to your code repository.
6. Modify the `.env` file to point to Cloud Spanner again and apply the latest migration(s) with
   `npx prisma migrate deploy`.
