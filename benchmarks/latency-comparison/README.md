# Latency Comparisons

This directory contains comparisons between using PostgreSQL drivers with PGAdapter and using
native Cloud Spanner client libraries and drivers. The tests in this directory focus on potential
latency difference between using PGAdapter or not, and therefore use simple statements that
operate on a single row at a time.

## Setup

All tests in this directory use a database with a single table. Follow these steps to create a
database that you can use for these tests:

1. Set up some environment variables. These will be used by all following steps.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export GOOGLE_CLOUD_PROJECT=my-project
export SPANNER_INSTANCE=my-instance
export SPANNER_DATABASE=my-database
```

2. (Optional) Create the Cloud Spanner PostgreSQL-dialect database if it does not already exist:

```shell
gcloud spanner databases create ${SPANNER_DATABASE} --instance=${SPANNER_INSTANCE} --database-dialect=POSTGRESQL
```

3. Start PGAdapter and PostgreSQL using Docker Compose. This will start both PGAdapter and PostgreSQL
   in a Docker container and connect both to the same Docker network.

```shell
docker-compose up -d
```

4. Create the test table in the Cloud Spanner database:

```shell
docker run -it --rm \
  --network latency-comparison_default \
  postgres psql -h pgadapter -c "create table if not exists latency_test (
    col_bigint bigint not null primary key,
    col_bool bool,
    col_bytea bytea,
    col_float8 float8,
    col_int int,
    col_numeric numeric,
    col_timestamptz timestamptz,
    col_date date,
    col_varchar varchar(100),
    col_jsonb jsonb
);"
```

5. Generate some random test data that can be used for the benchmarking. We do this by executing
   a query on PostgreSQL that generates 100,000 random rows and copies these to Cloud Spanner.

```shell
docker run -it --rm \
  --network latency-comparison_default \
  postgres bash -c "
    PGPASSWORD=mysecret psql -h postgres -U postgres \
      -c \"copy (
        select i::bigint,
        random() > 0.5,
        (SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
        FROM generate_series(1, (random() * 2000)::int + 10)),
        (random() * 1000000.1)::float8,
        (random() * 1000000)::int,
        (random() * 999999.99)::numeric,
        (timestamptz '1900-01-01 00:00:00Z' + random() * interval '200 year')::timestamptz,
        (date '1900-01-01 00:00:00Z' + random() * interval '200 year')::date,
        to_char(i, 'fm00000000'),
        null::jsonb
        from generate_series(1, 100000) s(i)
      ) to stdout binary\" \
      | psql -h pgadapter \
      -c \"set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy latency_test from stdin binary;\"
  "
```

6. Shut down PGAdapter and PostgreSQL.

```shell
docker-compose down
```
