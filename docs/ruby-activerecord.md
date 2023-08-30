# PGAdapter - Ruby ActiveRecord Connection Options

## Limitations
PGAdapter has experimental support for Ruby ActiveRecord 7.x with Cloud Spanner PostgreSQL databases.
Developing new applications using Ruby ActiveRecord 7.x is possible as long as the listed limitations
are taken into account.
Porting an existing application from PostgreSQL to Cloud Spanner is likely to require code changes.

See [Limitations](../samples/ruby/activerecord/README.md#limitations) in the `activerecord`
sample directory for a full list of limitations.

## Usage

First start PGAdapter:

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

Then connect to PGAdapter and use `ActiveRecord` using the connection parameters listed below.
Note the following additional options:
1. Configure ActiveRecord to use `timestamptz` as the default `datetime_type`.
2. Disable datetime with precision in ActiveRecord. This prevents ActiveRecord from trying to use
   column types like `timestamptz(6)`.
3. `advisory_locks: false` instructs ActiveRecord not to use `advisory_locks` for migrations.
   PGAdapter does not support `advisory_locks`.
4. `"spanner.ddl_transaction_mode": "AutocommitExplicitTransaction"` instructs PGAdapter to
   automatically commit the active transaction and start a DDL batch if a DDL statement is encountered
   in a transaction. ActiveRecord uses DDL transactions for migrations. This setting is not needed
   if you do not execute any migrations.
5. `"spanner.emulate_pg_class_tables": "true"` instructs PGAdapter to emulate `pg_class` tables that
   are needed for ActiveRecord to detect the tables and columns in the database.

```ruby
# Make sure that the PostgreSQL-adapter uses timestamptz without any type modifiers.
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.datetime_type = :timestamptz
module ActiveRecord::ConnectionAdapters
  class PostgreSQLAdapter
    def supports_datetime_with_precision?
      false
    end
  end
end

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  database: ENV['PGDATABASE'] || 'my-database',
  host: ENV['PGHOST'] || 'localhost',
  port: ENV['PGPORT'] || '5432',
  pool: 5,
  # Advisory locks are not supported by PGAdapter.
  advisory_locks: false,
  
  # These settings ensure that migrations and schema inspections work.
  # The 'AutocommitExplicitTransaction' DDL-mode converts PostgreSQL DDL transactions
  # to 
  variables: {
    "spanner.ddl_transaction_mode": "AutocommitExplicitTransaction",
    "spanner.emulate_pg_class_tables": "true"
  },
)
```

## Full Sample and Limitations
[This directory](../samples/ruby/activerecord) contains a full sample of how to work with
`Ruby ActiveRecord` with Cloud Spanner and PGAdapter. The sample readme file also lists the
[current limitations](../samples/ruby/activerecord/README.md#limitations) when working with
`ActiveRecord`.
