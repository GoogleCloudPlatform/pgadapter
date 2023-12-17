#!/bin/bash
set -euox pipefail

echo "Starting Task #${CLOUD_RUN_TASK_INDEX}, Attempt #${CLOUD_RUN_TASK_ATTEMPT}..."
EXECUTED_AT=`date +"%Y-%m-%dT%T"`

DATABASES=$(gcloud spanner databases list --instance $SPANNER_INSTANCE --filter="name:${SPANNER_DATABASE}")
if [[ "$DATABASES" != *"$SPANNER_DATABASE"* ]]; then
  gcloud spanner databases create $SPANNER_DATABASE --instance=$SPANNER_INSTANCE --database-dialect='POSTGRESQL'
fi
SPANNER_PROJECT=$(gcloud --quiet config get project)

cd pgadapter
java -jar pgadapter.jar -p $SPANNER_PROJECT -i $SPANNER_INSTANCE -enable_otel -r="minSessions=1000;maxSessions=1000;numChannels=20" &
sleep 6
export PGDATABASE=$SPANNER_DATABASE
psql -h localhost -c "CREATE TABLE IF NOT EXISTS usertable (
                   YCSB_KEY VARCHAR(255) PRIMARY KEY,
                   FIELD0 TEXT, FIELD1 TEXT,
                   FIELD2 TEXT, FIELD3 TEXT,
                   FIELD4 TEXT, FIELD5 TEXT,
                   FIELD6 TEXT, FIELD7 TEXT,
                   FIELD8 TEXT, FIELD9 TEXT
                 ); CREATE TABLE IF NOT EXISTS run (
                  executed_at     varchar,
                  deployment      varchar,
                  workload        varchar,
                  threads         bigint,
                  batch_size      bigint,
                  operation_count bigint,
                  run_time        bigint,
                  throughput      float,
                  read_min        float,
                  read_max        float,
                  read_avg        float,
                  read_p50        float,
                  read_p95        float,
                  read_p99        float,
                  update_min      float,
                  update_max      float,
                  update_avg      float,
                  update_p50      float,
                  update_p95      float,
                  update_p99      float,
                  insert_min      float,
                  insert_max      float,
                  insert_avg      float,
                  insert_p50      float,
                  insert_p95      float,
                  insert_p99      float,
                  primary key (executed_at, deployment, workload, threads, batch_size, operation_count)
                );"

cd /ycsb

cat <<EOT >> tcp.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost:5432/$SPANNER_DATABASE
db.user=
db.passwd=
EOT

cat <<EOT >> uds.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost/$SPANNER_DATABASE?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory\$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.5432
db.user=
db.passwd=
EOT

cat <<EOT >> cloudspanner.properties
db.driver=com.google.cloud.spanner.jdbc.JdbcDriver
db.url=jdbc:cloudspanner:/projects/$SPANNER_PROJECT/instances/$SPANNER_INSTANCE/databases/$SPANNER_DATABASE?numChannels=20
db.user=
db.passwd=
EOT

psql -h localhost -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"

# Load workloada
./bin/ycsb load jdbc -P workloads/workloada \
  -threads 20 \
  -p recordcount=1000000 \
  -p db.batchsize=200 \
  -p jdbc.batchupdateapi=true \
  -P tcp.properties \
  -cp "jdbc-binding/lib/*" \
  > ycsb.log

# Run workloads a, b, c, f, and d.
for WORKLOAD in a b c f d
do
  for DEPLOYMENT in java_tcp java_uds cloud_spanner
  do
    if [ $DEPLOYMENT == 'java_tcp' ]
    then
      CONN=tcp.properties
    elif [ $DEPLOYMENT == 'java_uds' ]
    then
      CONN=uds.properties
    elif [ $DEPLOYMENT == 'cloud_spanner' ]
    then
      CONN=cloudspanner.properties
    else
      CONN=uds.properties
    fi
    for THREADS in 1 5 20 50 200
    do
      OPERATION_COUNT=`expr $THREADS \* 1000`
      for BATCH_SIZE in 1 10 50
      do
        if [ $BATCH_SIZE == 1 ]
        then
          BATCH_API=false
        else
          BATCH_API=true
        fi
        ./bin/ycsb run jdbc -P workloads/workload$WORKLOAD \
          -threads $THREADS \
          -p hdrhistogram.percentiles=50,95,99 \
          -p operationcount=$OPERATION_COUNT \
          -p recordcount=1000000 \
          -p db.batchsize=$BATCH_SIZE \
          -p jdbc.batchupdateapi=$BATCH_API \
          -P $CONN \
          -cp "jdbc-binding/lib/*" \
          > ycsb.log

        OVERALL_RUNTIME=$(grep '\[OVERALL\], RunTime(ms), ' ycsb.log | sed 's/^.*, //' || echo null)
        OVERALL_THROUGHPUT=$(grep '\[OVERALL\], Throughput(ops/sec), ' ycsb.log | sed 's/^.*, //' || echo null)

        READ_AVG=$(grep '\[READ\], AverageLatency(us), ' ycsb.log | sed 's/^.*, //' | sed "s/NaN/'NaN'/" || echo null)
        READ_MIN=$(grep '\[READ\], MinLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        READ_MAX=$(grep '\[READ\], MaxLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        READ_P50=$(grep '\[READ\], 50thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        READ_P95=$(grep '\[READ\], 95thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        READ_P99=$(grep '\[READ\], 99thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)

        UPDATE_AVG=$(grep '\[UPDATE\], AverageLatency(us), ' ycsb.log | sed 's/^.*, //' | sed "s/NaN/'NaN'/" || echo null)
        UPDATE_MIN=$(grep '\[UPDATE\], MinLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        UPDATE_MAX=$(grep '\[UPDATE\], MaxLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        UPDATE_P50=$(grep '\[UPDATE\], 50thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        UPDATE_P95=$(grep '\[UPDATE\], 95thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        UPDATE_P99=$(grep '\[UPDATE\], 99thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)

        INSERT_AVG=$(grep '\[INSERT\], AverageLatency(us), ' ycsb.log | sed 's/^.*, //' | sed "s/NaN/'NaN'/" || echo null)
        INSERT_MIN=$(grep '\[INSERT\], MinLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        INSERT_MAX=$(grep '\[INSERT\], MaxLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        INSERT_P50=$(grep '\[INSERT\], 50thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        INSERT_P95=$(grep '\[INSERT\], 95thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)
        INSERT_P99=$(grep '\[INSERT\], 99thPercentileLatency(us), ' ycsb.log | sed 's/^.*, //' || echo null)

        psql -h localhost \
          -c "insert into run (executed_at, deployment, workload, threads, batch_size, operation_count, run_time, throughput,
                               read_min, read_max, read_avg, read_p50, read_p95, read_p99,
                               update_min, update_max, update_avg, update_p50, update_p95, update_p99,
                               insert_min, insert_max, insert_avg, insert_p50, insert_p95, insert_p99) values
                               ('$EXECUTED_AT', '$DEPLOYMENT', '$WORKLOAD', $THREADS, $BATCH_SIZE, $OPERATION_COUNT, $OVERALL_RUNTIME, $OVERALL_THROUGHPUT,
                                $READ_MIN, $READ_MAX, $READ_AVG, $READ_P50, $READ_P95, $READ_P99,
                                $UPDATE_MIN, $UPDATE_MAX, $UPDATE_AVG, $UPDATE_P50, $UPDATE_P95, $UPDATE_P99,
                                $INSERT_MIN, $INSERT_MAX, $INSERT_AVG, $INSERT_P50, $INSERT_P95, $INSERT_P99)"
      done
    done
  done
done
