create table if not exists latency_test (
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
);
