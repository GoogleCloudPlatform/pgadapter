START BATCH DDL;

CREATE TABLE pgbench_accounts (
aid integer primary key  NOT NULL,            
bid integer  NULL,                
abalance integer  NULL,           
filler varchar(84)  NULL);

CREATE TABLE pgbench_branches (
bid integer primary key  NOT NULL,            
bbalance integer  NULL,           
filler varchar(88)  NULL);

CREATE TABLE pgbench_history (       
tid integer  NOT NULL DEFAULT -1,                      
bid integer  NOT NULL DEFAULT -1,                      
aid integer  NOT NULL DEFAULT -1,                      
delta integer  NULL,                    
mtime timestamptz  NULL,
filler varchar(22)  NULL,
primary key (tid, bid, aid)
);

CREATE TABLE pgbench_tellers (
tid integer  primary key NOT NULL,           
bid integer  NULL,               
tbalance integer  NULL,          
filler varchar(84)  NULL);

RUN BATCH;


START BATCH DDL;
DROP TABLE pgbench_history;
DROP TABLE pgbench_tellers;
DROP TABLE pgbench_branches;
DROP TABLE pgbench_accounts;
RUN BATCH;


pgbench "host=localhost port=5433 dbname=knut-test-db options='-c spanner.ignore_transactions=on'" -i -Ig
pgbench "host=localhost port=5433 dbname=knut-test-db"


psql -h localhost -p 5433 -d knut-test-db -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'" -c "delete from pgbench_accounts"
