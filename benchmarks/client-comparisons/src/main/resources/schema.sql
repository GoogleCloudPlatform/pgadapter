START BATCH DDL;

create table benchmark_all_types (
  id varchar(36) primary key default spanner.generate_uuid(),
  col_bigint bigint,
  col_bool bool,
  col_bytea bytea,
  col_float8 float8,
  col_numeric numeric,
  col_timestamptz timestamptz,
  col_date date,
  col_varchar varchar,
  col_jsonb jsonb,
  col_array_bigint bigint[],
  col_array_bool bool[],
  col_array_bytea bytea[],
  col_array_float8 float8[],
  col_array_numeric numeric[],
  col_array_timestamptz timestamptz[],
  col_array_date date[],
  col_array_varchar varchar[],
  col_array_jsonb jsonb[]
);

create table benchmark_results (
  name varchar,
  executed_at timestamptz,
  parallelism bigint,
  avg numeric,
  p50 numeric,
  p90 numeric,
  p95 numeric,
  p99 numeric,
  total numeric,
  primary key (name, executed_at, parallelism)
);

RUN BATCH;
