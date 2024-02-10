--liquibase formatted sql

--changeset loite:2
create table ticket_sales (
    id bigint not null primary key,
    -- "concerts" uses a primary key with an automatically generated shard_id.
    concert_shard_id bigint generated always as (mod(concert_id, '2048'::bigint)) stored not null,
    concert_id       bigint not null,
    customer_name    varchar not null,
    price            decimal not null,
    created_at       timestamptz,
    updated_at       timestamptz,
    constraint fk_ticket_sales_concerts foreign key (concert_shard_id, concert_id) references concerts (shard_id, id)
);

-- Create a bit-reversed sequence that will be used to generate identifiers for the ticket_sales table.
-- See also https://cloud.google.com/spanner/docs/reference/postgresql/data-definition-language#create_sequence
create sequence ticket_sale_seq
    bit_reversed_positive
    skip range 1 1000
    start counter with 50000
;

--rollback drop sequence if exists ticket_sale_seq;
--rollback drop table if exists ticket_sales;
