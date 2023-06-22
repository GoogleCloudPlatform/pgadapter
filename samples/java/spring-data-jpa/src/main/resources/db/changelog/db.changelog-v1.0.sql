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

create table if not exists tracks (
    id           varchar(36) not null,
    track_number bigint not null,
    title        varchar not null,
    sample_rate  float8 not null,
    created_at   timestamptz,
    updated_at   timestamptz,
    primary key (id, track_number)
) interleave in parent albums on delete cascade;
