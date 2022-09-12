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
    singer_id        varchar not null,
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
    venue_id    varchar not null,
    singer_id   varchar not null,
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
