-- Create a schema for read-only foreign tables.
drop schema if exists _public_read_only cascade;
create schema _public_read_only;
SET search_path TO _public_read_only;

-- Create the foreign data wrapper extension.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create a server. It is highly recommended to create a 'read-only' server if you are only going to
-- query the foreign table. This will ensure that Cloud Spanner uses a read-only transaction for any
-- query that is executed on the foreign table.
CREATE SERVER IF NOT EXISTS pgadapter_read_only FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        -- This is the host name of the PGAdapter container.
        host 'pgadapter', port '5432',
        -- This instructs PGAdapter to use read-only transactions.
        options '-c spanner.readonly=true');

-- We must create a user mapping for the local server. The user is ignored by PGAdapter.
CREATE USER MAPPING IF NOT EXISTS FOR public SERVER pgadapter_read_only
    OPTIONS (user 'not_used', password_required 'false');

-- Now import all tables as foreign tables to the read-only schema.
IMPORT FOREIGN SCHEMA public from server pgadapter_read_only into _public_read_only;

---------------------------------------------------------------------------------------------------

-- Create a schema for read/write foreign tables.
drop schema if exists _public_read_write cascade;
create schema _public_read_write;
SET search_path TO _public_read_write;

-- Create a server that uses the default read/write mode. This server will by default use a
-- read/write transaction for all operations.
CREATE SERVER IF NOT EXISTS pgadapter_read_write FOREIGN DATA WRAPPER postgres_fdw
    -- This is the host name of the PGAdapter container.
    OPTIONS (host 'pgadapter', port '5432');

-- We must create a user mapping for the local server. The user is ignored by PGAdapter.
CREATE USER MAPPING IF NOT EXISTS FOR public SERVER pgadapter_read_write
    OPTIONS (user 'not_used', password_required 'false');

-- Now import all tables as foreign tables to the read/write schema.
IMPORT FOREIGN SCHEMA public from server pgadapter_read_write into _public_read_write;
