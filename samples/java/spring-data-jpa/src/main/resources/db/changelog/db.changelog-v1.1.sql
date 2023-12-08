--liquibase formatted sql

--changeset loite:2
create table lob_test (
    id varchar(36) not null primary key,
    lob_bytea bytea,
    lob_oid bigint,
    created_at timestamptz,
    updated_at timestamptz
);

--rollback drop table if exists lob_test;
