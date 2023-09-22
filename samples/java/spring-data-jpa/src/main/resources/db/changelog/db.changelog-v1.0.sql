--liquibase formatted sql

--changeset loite:1
create table singers (
    id varchar(36) not null primary key,
    first_name varchar(100),
    last_name varchar(200),
    full_name varchar(300) generated always as (
        CASE WHEN first_name IS NULL THEN last_name
             WHEN last_name IS NULL THEN first_name
             ELSE first_name || ' ' || last_name
            END) stored,
    active boolean,
    created_at timestamptz,
    updated_at timestamptz
);

create table albums (
    id varchar(36) not null primary key,
    title varchar(200),
    marketing_budget numeric,
    release_date date,
    cover_picture bytea,
    singer_id varchar(36) not null,
    created_at timestamptz,
    updated_at timestamptz,
    constraint fk_albums_singers foreign key (singer_id) references singers (id)
);

create table tracks (
    id           varchar(36) not null,
    track_number bigint not null,
    title        varchar not null,
    sample_rate  float8 not null,
    created_at   timestamptz,
    updated_at   timestamptz,
    primary key (id, track_number)
) interleave in parent albums on delete cascade;

-- This table is used to generate sequential primary key values.
-- Note that one table can be used to generate primary keys for multiple entities.
-- It is highly recommended to use a separate generator for each entity (that is; use a separate row
-- in this table for each entity).
create table seq_ids (
    seq_id varchar not null primary key,
    seq_value bigint not null
);

create table venues (
    -- shard_id is not visible to Hibernate and is only here to prevent write-hotspots in Cloud Spanner.
    -- See https://cloud.google.com/spanner/docs/generated-column/how-to#primary-key-generated-column
    -- for more information.
    shard_id    bigint generated always as (mod(id, '2048'::bigint)) stored not null,
    id          bigint not null,
    name        varchar not null,
    description jsonb not null,
    created_at  timestamptz,
    updated_at  timestamptz,
    primary key (shard_id, id)
);

create table concerts (
    -- shard_id is not visible to Hibernate and is only here to prevent write-hotspots in Cloud Spanner.
    -- See https://cloud.google.com/spanner/docs/generated-column/how-to#primary-key-generated-column
    -- for more information.
    shard_id       bigint generated always as (mod(id, '2048'::bigint)) stored not null,
    id             bigint not null,
    -- venue_shard_id is not visible to Hibernate.
    venue_shard_id bigint generated always as (mod(venue_id, '2048'::bigint)) stored not null,
    venue_id       bigint not null,
    singer_id      varchar not null,
    name           varchar not null,
    start_time     timestamptz not null,
    end_time       timestamptz not null,
    created_at     timestamptz,
    updated_at     timestamptz,
    constraint fk_concerts_venues  foreign key (venue_shard_id, venue_id)  references venues (shard_id, id),
    constraint fk_concerts_singers foreign key (singer_id) references singers (id),
    constraint chk_end_time_after_start_time check (end_time > start_time),
    primary key (shard_id, id)
);

--rollback drop table if exists seq_ids;
--rollback drop table if exists concerts;
--rollback drop table if exists venues;
--rollback drop table if exists tracks;
--rollback drop table if exists albums;
--rollback drop table if exists singers;
