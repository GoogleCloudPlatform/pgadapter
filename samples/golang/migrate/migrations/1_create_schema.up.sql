-- Use DDL batching to improve execution speed.
-- See https://cloud.google.com/spanner/docs/pgadapter-session-mgmt-commands#batch-statements
-- for more information.
start batch ddl;

create table numbers (num bigint primary key, name varchar);
create table prime_numbers (num bigint primary key, name varchar, n bigint);

run batch;
