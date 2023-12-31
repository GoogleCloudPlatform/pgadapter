start batch ddl;

drop table if exists benchmark_all_types;
drop table if exists benchmark_results;

run batch;
