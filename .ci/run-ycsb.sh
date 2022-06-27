for OPERATION_COUNT in 1000
do
  for THREADS in 1 5
  do
    for BATCH_SIZE in 1 5
    do
      ./bin/ycsb ${YCSB_COMMAND} jdbc -P workloads/${WORKLOAD} \
          -threads ${THREADS} \
          -p operationcount=${OPERATION_COUNT} \
          -p jdbc.batchupdateapi=true \
          -P ${YCSB_PROPERTY_FILE} \
          -cp "jdbc-binding/lib/*" > output.txt

      RUNTIME=$(sed -En 's/\[OVERALL\], RunTime\(ms\), (.+)$/\1/p' output.txt)
      THROUGHPUT=$(sed -En 's/\[OVERALL\], Throughput\(ops\/sec\), (.+)$/\1/p' output.txt)
      READ_AVG=$(sed -En 's/\[READ\], AverageLatency\(us\), (.+)$/\1/p' output.txt)
      READ_P95=$(sed -En 's/\[READ\], 95thPercentileLatency\(us\), (.+)$/\1/p' output.txt)
      READ_P99=$(sed -En 's/\[READ\], 99thPercentileLatency\(us\), (.+)$/\1/p' output.txt)
      INSERT_AVG=$(sed -En 's/\[INSERT\], AverageLatency\(us\), (.+)$/\1/p' output.txt)
      INSERT_P95=$(sed -En 's/\[INSERT\], 95thPercentileLatency\(us\), (.+)$/\1/p' output.txt)
      INSERT_P99=$(sed -En 's/\[INSERT\], 99thPercentileLatency\(us\), (.+)$/\1/p' output.txt)

      psql -h /tmp -p 5433 -c "
          insert into run (deployment, workload, threads, batch_size, operation_count, run_time, throughput,
                           read_avg, read_p95, read_p99, insert_avg, insert_p95, insert_p99)
          values (${DEPLOYMENT}, ${WORKLOAD}, ${THREADS}, ${BATCH_SIZE}, ${OPERATION_COUNT}, ${RUNTIME}, ${THROUGHPUT},
                  ${READ_AVG}, ${READ_P95}, ${READ_P99}, ${INSERT_AVG}, ${INSERT_P95}, ${INSERT_P99});"

      psql -h /tmp -p 5433 -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"
    done
  done
done
