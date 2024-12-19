-- Executing the schema drop in a batch will improve execution speed.
start batch ddl;

drop table if exists ticket_sales;
drop sequence if exists ticket_sale_seq;
drop table if exists concerts;
drop table if exists venues;
drop table if exists tracks;
drop table if exists albums;
drop table if exists singers;

run batch;
