
psql -h localhost -p 9000 -d postgres -U postgres \
  -c "copy (
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
  ) to stdout binary" \
  | psql -h localhost -p 9001 -d latency-test \
  -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy latency_test from stdin binary;"

