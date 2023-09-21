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

-- This creates the bit-reversed sequence that is used to create primary key values for the Venue
-- entity. The values that are set here correspond with the values that are set in the annotations
-- for the id property in the Venue entity class file.
-- Note that Cloud Spanner bit-reversed sequences do not support setting an increment_size. Instead,
-- this is handled by the EnhancedBitReversedSequenceStyleGenerator class that is set as the
-- generator to use for the entity.
-- See also https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#create_sequence.
create sequence venue_id_sequence
    bit_reversed_positive -- This is a Cloud Spanner specific extension to open-source PostgreSQL.
    skip range 10000 20000
    start counter with 5000;

create table venues (
    id          bigint not null primary key,
    name        varchar not null,
    description jsonb not null,
    created_at  timestamptz,
    updated_at  timestamptz
);

create sequence concert_id_sequence bit_reversed_positive;

create table concerts (
    id             bigint not null primary key,
    venue_id       bigint not null,
    singer_id      varchar not null,
    name           varchar not null,
    start_time     timestamptz not null,
    end_time       timestamptz not null,
    created_at     timestamptz,
    updated_at     timestamptz,
    constraint fk_concerts_venues  foreign key (venue_id)  references venues (id),
    constraint fk_concerts_singers foreign key (singer_id) references singers (id),
    constraint chk_end_time_after_start_time check (end_time > start_time)
);

--rollback drop sequence if exists concert_id_sequence;
--rollback drop sequence if exists venue_id_sequence;
--rollback drop table if exists concerts;
--rollback drop table if exists venues;
--rollback drop table if exists tracks;
--rollback drop table if exists albums;
--rollback drop table if exists singers;
