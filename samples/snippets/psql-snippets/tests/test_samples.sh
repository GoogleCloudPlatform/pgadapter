#!/bin/bash

RESULT=0
MESSAGE=
CONTAINER=$(docker run -d --rm -p 5432 gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator)
HOST_AND_PORT=$(docker port "$CONTAINER" 5432)

export PGHOST=localhost
export PGPORT="${HOST_AND_PORT##*:}"
export PGDATABASE="example-db"
sleep 2

MESSAGE=""

if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh create_tables $'CREATE\nCREATE
Created Singers & Albums tables in database: [example-db]'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh create_connection $'    hello     \n--------------\n Hello world!\n(1 row)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh write_data_with_dml $'INSERT 0 4
4 records inserted'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh write_data_with_dml_batch $'PREPARE
INSERT 0 1
INSERT 0 1
INSERT 0 1
3 records inserted'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh write_data_with_copy $'COPY 5\nCOPY 5
Copied singers and albums'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh query_data $' singer_id | album_id |       album_title       
-----------+----------+-------------------------
         1 |        2 | Go, Go, Go
         2 |        2 | Forever Hold Your Peace
         1 |        1 | Total Junk
         2 |        1 | Green
         2 |        3 | Terrified
(5 rows)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh query_data_with_parameter $'PREPARE
 singer_id | first_name | last_name 
-----------+------------+-----------
        12 | Melissa    | Garcia
(1 row)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh statement_timeout $'SET
 singer_id | album_id | album_title 
-----------+----------+-------------
(0 rows)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh add_column $'ALTER\nAdded marketing_budget column'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh ddl_batch $'CREATE\nCREATE
Added venues and concerts tables'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh update_data_with_copy $'SET\nCOPY 2\nCopied albums using upsert'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh query_data_with_new_column $' singer_id | album_id | marketing_budget 
-----------+----------+------------------
         1 |        1 |           100000
         1 |        2 |                 
         2 |        1 |                 
         2 |        2 |           500000
         2 |        3 |                 
(5 rows)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh update_data_with_transaction $'BEGIN\nUPDATE 1\nUPDATE 1\nCOMMIT
Transferred marketing budget from Album 2 to Album 1'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh tags $'BEGIN
SET
SET
 marketing_budget 
------------------
           300000
(1 row)

SET
UPDATE 1
COMMIT
Reduced marketing budget'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh read_only_transaction $'BEGIN
SET
 singer_id | album_id |       album_title       
-----------+----------+-------------------------
         1 |        1 | Total Junk
         1 |        2 | Go, Go, Go
         2 |        1 | Green
         2 |        2 | Forever Hold Your Peace
         2 |        3 | Terrified
(5 rows)

 singer_id | album_id |       album_title       
-----------+----------+-------------------------
         2 |        2 | Forever Hold Your Peace
         1 |        2 | Go, Go, Go
         2 |        1 | Green
         2 |        3 | Terrified
         1 |        1 | Total Junk
(5 rows)

COMMIT'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh data_boost $'SET
 singer_id | first_name | last_name 
-----------+------------+-----------
         2 | Catalina   | Smith
         4 | Lea        | Martin
        12 | Melissa    | Garcia
        14 | Jacqueline | Long
        16 | Sarah      | Wilson
        18 | Maya       | Patel
         1 | Marc       | Richards
         3 | Alice      | Trentor
         5 | David      | Lomond
        13 | Russel     | Morales
        15 | Dylan      | Shaw
        17 | Ethan      | Miller
(12 rows)'
fi
if [ "$MESSAGE" == "" ]; then
  source ./test_sample.sh partitioned_dml $'SET\nUPDATE 3\nUpdated albums using Partitioned DML'
fi


STOPPED_CONTAINER=$(docker container stop "$CONTAINER")
if [ "$STOPPED_CONTAINER" != "$CONTAINER" ]; then
  RESULT=1
  MESSAGE="Failed to stop container"
fi

if [ "$MESSAGE" != "" ]; then
  echo "$MESSAGE"
  echo "$MESSAGE" | tr ' ' '.$'
  exit "$RESULT"
fi
