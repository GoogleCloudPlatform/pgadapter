package main

import (
	"cloud.google.com/pgadapter-latency-benchmark/runners"
	"fmt"
	"github.com/montanaflynn/stats"
)

func main() {
	numExecutions := 500
	db := "projects/cloud-spanner-pg-adapter/instances/pgadapter-ycsb-regional-test/databases/latency-test"
	sql := "select col_varchar from latency_test where col_bigint=$1"

	fmt.Println("Running client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(db, sql, numExecutions)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx benchmark")
	pgxRunTimes, err := runners.RunPgx(db, sql, numExecutions)
	if err != nil {
		panic(err)
	}
	printReport("Go Client Library", clientLibRunTimes)
	printReport("pgx with PGAdapter", pgxRunTimes)
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
