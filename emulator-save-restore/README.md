# Save and restore an emulator database

This folder contains scripts to save a database created in the emulator and
restore it when the emulator needs to be restarted.

## Try it out

1. Follow the
   [instructions](https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/emulator.md#google-cloud-spanner-pgadapter---emulator)
to setup PGAdapter and Spanner Emulator.

2. Use the emulator database to store data.

3. Ensure the following environment variables are set:

```
# Project, instance and database for Cloud Spanner.
GOOGLE_CLOUD_PROJECT=my-project
SPANNER_INSTANCE=my-instance
SPANNER_DATABASE=my-database
```

```
PGADAPTER_HOST=localhost
PGADAPTER_PORT=9031
```

3. Save the schema and data in the emulator locally.

```shell
./save-emulator-database.sh

```

By default, the schema is saved in `./backup/schema.sql` and the data within
individual files per table in the `.backup/data/` folder.

4. Restart the emulator then re-create the instance and database.

5. Restore the data in the emulator from the locally stored sql files.

```shell
./restore-emulator-database.sh
```
