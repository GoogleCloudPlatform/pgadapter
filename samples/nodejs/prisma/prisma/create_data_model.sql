
-- Executing the schema creation in a batch will improve execution speed.
start batch ddl;

create table if not exists "Singer" (
    id          varchar not null primary key,
    "firstName" varchar,
    "lastName"  varchar not null,
    "fullName"  varchar generated always as (coalesce(concat("firstName", ' '::varchar, "lastName"), "lastName")) stored,
    active      boolean,
    "createdAt" timestamptz,
    "updatedAt" timestamptz
);

create table if not exists "Album" (
    id                varchar not null primary key,
    title             varchar not null,
    "marketingBudget" numeric,
    "releaseDate"     date,
    "coverPicture"    bytea,
    "singerId"        varchar not null,
    "createdAt"       timestamptz,
    "updatedAt"       timestamptz,
    constraint fk_albums_singers foreign key ("singerId") references "Singer" (id)
);

create table if not exists "Track" (
    id            varchar not null,
    "trackNumber" bigint not null,
    title         varchar not null,
    "sampleRate"  float8 not null,
    "createdAt"   timestamptz,
    "updatedAt"   timestamptz,
    primary key (id, "trackNumber")
) interleave in parent "Album" on delete cascade;

create table if not exists "Venue" (
    id          varchar not null primary key,
    name        varchar not null,
    description jsonb not null,
    "createdAt" timestamptz,
    "updatedAt" timestamptz
);

create table if not exists "Concert" (
    id          varchar not null primary key,
    "venueId"   varchar not null,
    "singerId"  varchar not null,
    name        varchar not null,
    "startTime" timestamptz not null,
    "endTime"   timestamptz not null,
    "createdAt" timestamptz,
    "updatedAt" timestamptz,
    constraint fk_concerts_venues  foreign key ("venueId")  references "Venue"  (id),
    constraint fk_concerts_singers foreign key ("singerId") references "Singer" (id),
    constraint chk_end_time_after_start_time check ("endTime" > "startTime")
);

run batch;
