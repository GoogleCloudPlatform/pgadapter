## Generated Data

This folder contains a number of test files with generated data.

### all_types_data_small.txt

Generate using a local PostgreSQL server.

```shell
cat > copy_all_types.sql <<- EOM
copy (
select (random()*1000000000)::bigint, random()<0.5, md5(random()::text ||
clock_timestamp()::text)::bytea, (random()*123456789)::float4, random()*123456789,
(random()*999999)::int, (random()*999999)::numeric,
now()-random()*interval '50 year', (now()-random()*interval '50 year')::date,
md5(random()::text || clock_timestamp()::text)::varchar,
('{"key": "' || md5(random()::text || clock_timestamp()::text)::varchar || '"}')::jsonb
from generate_series(1, 100) s(i)) to stdout;
EOM
psql -h localhost -p 5432 -f copy_all_types.sql > all_types_data_small.txt
```

### all_types_data.txt

Generate using a local PostgreSQL server.

```shell
cat > copy_all_types.sql <<- EOM
copy (
select (random()*1000000000)::bigint, random()<0.5, md5(random()::text ||
clock_timestamp()::text)::bytea, (random()*123456789)::float4, random()*123456789,
(random()*999999)::int, (random()*999999)::numeric,
now()-random()*interval '50 year', (now()-random()*interval '50 year')::date,
md5(random()::text || clock_timestamp()::text)::varchar,
('{"key": "' || md5(random()::text || clock_timestamp()::text)::varchar || '"}')::jsonb
from generate_series(1, 10000) s(i)) to stdout;
EOM
psql -h localhost -p 5432 -f copy_all_types.sql > all_types_data.txt
```

### all_array_types_data_small.txt

```shell
cat > copy_all_array_types_small.sql <<- EOM
copy (
select
    array[(random()*1000000000)::bigint, null, (random()*1000000000)::bigint],
    array[random()<0.5, null, random()<0.5],
    array[md5(random()::text ||clock_timestamp()::text)::bytea, null, md5(random()::text ||clock_timestamp()::text)::bytea],
    array[(random()*123456789)::float4, null, (random()*123456789)::float4],
    array[random()*123456789, null, random()*123456789],
    array[(random()*999999)::int, null, (random()*999999)::int],
    array[(random()*999999)::numeric, null, (random()*999999)::numeric],
    array[now()-random()*interval '50 year', null, now()-random()*interval '50 year'],
    array[(now()-random()*interval '50 year')::date, null, (now()-random()*interval '50 year')::date],
    array[md5(random()::text || clock_timestamp()::text)::varchar, null, md5(random()::text || clock_timestamp()::text)::varchar],
    array[('{"key": "' || md5(random()::text || clock_timestamp()::text)::varchar || '"}')::jsonb, null, ('{"key": "' || md5(random()::text || clock_timestamp()::text)::varchar || '"}')::jsonb]
from generate_series(1, 100) s(i)) to stdout;
EOM
psql -f copy_all_array_types_small.sql > all_array_types_data_small.txt
```