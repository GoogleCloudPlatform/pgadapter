-- Use DDL batching to improve execution speed.
-- See https://cloud.google.com/spanner/docs/pgadapter-session-mgmt-commands#batch-statements
-- for more information.
start batch ddl;

drop table numbers;
drop table prime_numbers;

run batch;
