package main

import (
	"cloud.google.com/pgadapter-latency-benchmark/runners"
	"fmt"
	"github.com/montanaflynn/stats"
)

func main() {
	numExecutions := 100
	db := "projects/cloud-spanner-pg-adapter/instances/pgadapter-ycsb-regional-test/databases/latency-test"
	sql := "select col_varchar from latency_test where col_bigint=$1"

	fmt.Println("Running client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(db, sql, numExecutions)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx benchmark using TCP")
	pgxTcpRunTimes, err := runners.RunPgx(db, sql, numExecutions, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx benchmark using Unix Domain Socket")
	pgxUdsRunTimes, err := runners.RunPgx(db, sql, numExecutions, true)
	if err != nil {
		panic(err)
	}
	printReport("Go Client Library", clientLibRunTimes)
	printReport("pgx with PGAdapter over TCP", pgxTcpRunTimes)
	printReport("pgx with PGAdapter over UDS", pgxUdsRunTimes)
}

func printReport(header string, runTimes []float64) {
	sum, err := stats.Sum(runTimes)
	if err != nil {
		panic(err)
	}
	mean, err := stats.Mean(runTimes)
	if err != nil {
		panic(err)
	}
	p50, err := stats.Percentile(runTimes, 50.0)
	if err != nil {
		panic(err)
	}
	p95, err := stats.Percentile(runTimes, 95.0)
	if err != nil {
		panic(err)
	}
	fmt.Println()
	fmt.Println(header)
	fmt.Printf("Total executions: %v\n", len(runTimes))
	fmt.Printf("Total runtime: %fms\n", sum)
	fmt.Printf("Average: %fms\n", mean)
	fmt.Printf("p50: %fms\n", p50)
	fmt.Printf("p95: %fms\n", p95)
	fmt.Println()
}
