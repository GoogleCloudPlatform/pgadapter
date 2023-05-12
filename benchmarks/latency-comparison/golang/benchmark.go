package main

import (
	"flag"
	"fmt"

	"cloud.google.com/pgadapter-latency-benchmark/runners"
	"github.com/montanaflynn/stats"
)

func main() {
	numOperations := flag.Int("operations", 1000, "The number of operations that that each client will run. The total number of operations is clients*operations.")
	numClients := flag.Int("clients", 1, "The number of numClients that will be executing queries in parallel.")
	embedded := flag.Bool("embedded", false, "Starts an embedded PGAdapter container along with the benchmark.")
	flag.Parse()

	db := "projects/cloud-spanner-pg-adapter/instances/pgadapter-ycsb-regional-test/databases/latency-test"
	sql := "select col_varchar from latency_test where col_bigint=$1"

	fmt.Println("Running client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(db, sql, *numOperations, *numClients)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v5 benchmark using TCP")
	pgxTcpRunTimes, err := runners.RunPgx(db, sql, *numOperations, *numClients, false, *embedded)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v5 benchmark using Unix Domain Socket")
	pgxUdsRunTimes, err := runners.RunPgx(db, sql, *numOperations, *numClients, true, *embedded)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using TCP")
	pgxV4TcpRunTimes, err := runners.RunPgxV4(db, sql, *numOperations, *numClients, false, *embedded)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using Unix Domain Socket")
	pgxV4UdsRunTimes, err := runners.RunPgxV4(db, sql, *numOperations, *numClients, true, *embedded)
	if err != nil {
		panic(err)
	}
	printReport("Go Client Library", *numClients, clientLibRunTimes)
	printReport("pgx v5 with PGAdapter over TCP", *numClients, pgxTcpRunTimes)
	printReport("pgx v5 with PGAdapter over UDS", *numClients, pgxUdsRunTimes)
	printReport("pgx v4 with PGAdapter over TCP", *numClients, pgxV4TcpRunTimes)
	printReport("pgx v4 with PGAdapter over UDS", *numClients, pgxV4UdsRunTimes)
}

func printReport(header string, numClients int, runTimes []float64) {
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
	fmt.Printf("Number of clients: %v\n", numClients)
	fmt.Printf("Total operations: %v\n", len(runTimes))
	fmt.Printf("Average: %fms\n", mean)
	fmt.Printf("p50: %fms\n", p50)
	fmt.Printf("p95: %fms\n", p95)
	fmt.Println()
}
