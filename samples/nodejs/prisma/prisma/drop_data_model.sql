-- Executing the schema drop in a batch will improve execution speed.
start batch ddl;

drop table if exists "Concert";
drop table if exists "Venue";
drop table if exists "Track";
drop table if exists "Album";
drop table if exists "Singer";

run batch;
