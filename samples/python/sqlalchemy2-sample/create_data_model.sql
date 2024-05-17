
-- Executing the schema creation in a batch will improve execution speed.
start batch ddl;

create table if not exists singers (
    id         varchar not null primary key,
    version_id int not null,
    first_name varchar,
    last_name  varchar not null,
    full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
    active     boolean,
    created_at timestamptz,
    updated_at timestamptz
);

create table if not exists albums (
    id               varchar not null primary key,
    version_id       int not null,
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
    version_id   int not null,
    title        varchar not null,
    sample_rate  float8 not null,
    created_at   timestamptz,
    updated_at   timestamptz,
    primary key (id, track_number)
) interleave in parent albums on delete cascade;

create table if not exists venues (
    id          varchar not null primary key,
    version_id  int not null,
    name        varchar not null,
    description jsonb not null,
    created_at  timestamptz,
    updated_at  timestamptz
);

create table if not exists concerts (
    id          varchar not null primary key,
    version_id  int not null,
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

-- Create a bit-reversed sequence that will be used to generate identifiers for the ticket_sales table.
-- See also https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#create_sequence
-- Note that the 'bit_reversed_positive' keyword is required for Spanner.
create sequence if not exists ticket_sale_seq
    bit_reversed_positive
    skip range 1 1000
    start counter with 50000
;

create table if not exists ticket_sales (
    id bigint not null primary key default nextval('ticket_sale_seq'),
    version_id       int not null,
    concert_id       varchar not null,
    customer_name    varchar not null,
    price            decimal not null,
    seats            text[],
    created_at       timestamptz,
    updated_at       timestamptz,
    constraint fk_ticket_sales_concerts foreign key (concert_id) references concerts (id)
);

run batch;
