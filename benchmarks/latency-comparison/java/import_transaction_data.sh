
docker compose up -d


docker run -it --rm \
  --network java_default \
  postgres psql -h pgadapter -c "
-- Executing the schema creation in a batch will improve execution speed.
start batch ddl;

create table if not exists singers (
    id         varchar not null primary key,
    first_name varchar,
    last_name  varchar not null,
    full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
    active     boolean,
    created_at timestamptz,
    updated_at timestamptz
);

create table if not exists albums (
    id               varchar not null primary key,
    title            varchar not null,
    marketing_budget numeric,
    release_date     date,
    cover_picture    bytea,
    singer_id        varchar,
    created_at       timestamptz,
    updated_at       timestamptz,
    constraint fk_albums_singers foreign key (singer_id) references singers (id)
);

create table if not exists tracks (
    id           varchar not null,
    track_number bigint not null,
    title        varchar not null,
    sample_rate  float8 not null,
    created_at   timestamptz,
    updated_at   timestamptz,
    primary key (id, track_number)
) interleave in parent albums on delete cascade;

create table if not exists venues (
    id          varchar not null primary key,
    name        varchar not null,
    description varchar not null,
    created_at  timestamptz,
    updated_at  timestamptz
);

create table if not exists concerts (
    id          varchar not null primary key,
    venue_id    varchar,
    singer_id   varchar,
    name        varchar not null,
    start_time  timestamptz not null,
    end_time    timestamptz not null,
    created_at  timestamptz,
    updated_at  timestamptz,
    constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
    constraint fk_concerts_singers foreign key (singer_id) references singers (id),
    constraint chk_end_time_after_start_time check (end_time > start_time)
);

run batch;
  "

docker run -it --rm \
  --network java_default \
  postgres psql -h pgadapter \
  -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'" \
  -c "truncate table concerts" \
  -c "truncate table venues" \
  -c "truncate table tracks" \
  -c "truncate table albums" \
  -c "truncate table singers"


docker run -it --rm \
  --network java_default \
  postgres bash -c "
    PGPASSWORD=mysecret psql -h postgres -U postgres \
      -c \"copy (
        select gen_random_uuid()::varchar as id,
        gen_random_uuid()::varchar as first_name,
        gen_random_uuid()::varchar as last_name,
        random() > 0.5 as active,
        now()::timestamptz as created_at,
        now()::timestamptz as updated_at
        from generate_series(1, 100000) s(i)
      ) to stdout binary\" \
      | psql -h pgadapter \
      -c \"set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy singers (id, first_name, last_name, active, created_at, updated_at) from stdin binary;\"
  "

docker run -it --rm \
  --network java_default \
  postgres bash -c "
    PGPASSWORD=mysecret psql -h postgres -U postgres \
      -c \"copy (
        select gen_random_uuid()::varchar as id,
        gen_random_uuid()::varchar as title,
        (random()*1000000.0)::numeric as marketing_budget,
        (date '1900-01-01 00:00:00Z' + random() * interval '200 year')::date as release_date,
        (SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
         FROM generate_series(1, (random() * 2000)::int + 10)) as cover_picture,
        null::varchar as singer_id,
        now()::timestamptz as created_at,
        now()::timestamptz as updated_at
        from generate_series(1, 1000000) s(i)
      ) to stdout binary\" \
      | psql -h pgadapter \
      -c \"set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy albums from stdin binary;\"
  "

docker run -it --rm \
  --network java_default \
  postgres bash -c "
    PGPASSWORD=mysecret psql -h postgres -U postgres \
      -c \"copy (
        select gen_random_uuid()::varchar as id,
        gen_random_uuid()::varchar as name,
        gen_random_uuid()::varchar as description,
        now()::timestamptz as created_at,
        now()::timestamptz as updated_at
        from generate_series(1, 1000) s(i)
      ) to stdout binary\" \
      | psql -h pgadapter \
      -c \"set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy venues from stdin binary;\"
  "

docker run -it --rm \
  --network java_default \
  postgres bash -c "
    PGPASSWORD=mysecret psql -h postgres -U postgres \
      -c \"copy (
        select gen_random_uuid()::varchar as id,
        null::varchar as venue_id,
        null::varchar as singer_id,
        gen_random_uuid()::varchar as name,
        (timestamptz '1900-01-01 00:00:00Z' + random() * interval '120 year')::timestamptz as start_time,
        (timestamptz '2020-01-01 00:00:00Z' + random() * interval '120 year')::timestamptz as start_time,
        now()::timestamptz as created_at,
        now()::timestamptz as updated_at
        from generate_series(1, 1000000) s(i)
      ) to stdout binary\" \
      | psql -h pgadapter \
      -c \"set spanner.autocommit_dml_mode='partitioned_non_atomic'; copy concerts from stdin binary;\"
  "

docker compose down
