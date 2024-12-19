# Backup and restore a Spanner database

This folder contains scripts to backup a Spanner database and restore it using
local files.

**NOTE:** While these scripts may work for production databases, it is
currently intended for use with the Spanner emulator to achieve persistence, so
*it has not been tested for production use*.

## Try it out

1. For usage with the emulator, please follow the
   [instructions](https://github.com/GoogleCloudPlatform/pgadapter/blob/-/docs/emulator.md#google-cloud-spanner-pgadapter---emulator)
to setup PGAdapter and Spanner Emulator.

2. Create a Spanner database, then create a schema and store data.

3. Ensure the following environment variables are set:

```
# For production usage, the full path of the Google Cloud credentials file that
should be used for PGAdapter.
GOOGLE_APPLICATION_CREDENTIALS=/local/path/to/credentials.json

# Project, instance and database for Spanner.
GOOGLE_CLOUD_PROJECT=my-project
SPANNER_INSTANCE=my-instance
SPANNER_DATABASE=my-database

# Host and port the PGAdapter is deployed on.
PGADAPTER_HOST=localhost
PGADAPTER_PORT=5432
```

3. Save the schema and data of the Spanner database locally.

```shell
./backup-database.sh

```

By default, the schema is saved in `./backup/schema.sql` and the data within
individual files per table in the `./backup/data/` folder.

4. If using the emulator, restart it then re-create the instance and database.

5. Restore the schema and data back to Spanner from the locally stored sql files.

```shell
./restore-database.sh
```

The restore database can be different from the source database.

```shell
spanner_restore_database=new-database ./restore-database.sh
```

## Limitations

- The scripts currently only support backing up and restoring data in the
  'public' schema. They will not backup and restore the schema or data under
  named schemas.
