package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"

	"cloud.google.com/pgadapter-latency-benchmark/runners"
	"cloud.google.com/pgadapter-latency-benchmark/runners/pgadapter"
	"github.com/montanaflynn/stats"
)

func main() {
	db := flag.String("database", "projects/cloud-spanner-pg-adapter/instances/pgadapter-ycsb-regional-test/databases/latency-test", "The database to use for benchmarking.")
	numOperations := flag.Int("operations", 1000, "The number of operations that that each client will run. The total number of operations is clients*operations.")
	numClients := flag.Int("clients", 1, "The number of numClients that will be executing queries in parallel.")
	embedded := flag.Bool("embedded", true, "Starts an embedded PGAdapter container along with the benchmark. Setting this option will ignore any host or port settings for PGAdapter.")
	flag.Parse()

	// Start PGAdapter in a Docker container.
	project, instance, _, err := parseDatabaseName(*db)
	var port int
	if *embedded {
		var cleanup func()
		port, cleanup, err = pgadapter.StartPGAdapter(context.Background(), project, instance)
		if err != nil {
			panic(err)
		}
		defer cleanup()
	} else {
		port = 5432
	}

	sql := "select col_varchar from latency_test where col_bigint=$1"
	fmt.Println("Running client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(*db, sql, *numOperations, *numClients)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v5 benchmark using TCP")
	pgxTcpRunTimes, err := runners.RunPgx(*db, sql, *numOperations, *numClients, port, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v5 benchmark using Unix Domain Socket")
	pgxUdsRunTimes, err := runners.RunPgx(*db, sql, *numOperations, *numClients, port, true)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using TCP")
	pgxV4TcpRunTimes, err := runners.RunPgxV4(*db, sql, *numOperations, *numClients, port, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using Unix Domain Socket")
	pgxV4UdsRunTimes, err := runners.RunPgxV4(*db, sql, *numOperations, *numClients, port, true)
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

var (
	validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")
)

func parseDatabaseName(db string) (project, instance, database string, err error) {
	matches := validDBPattern.FindStringSubmatch(db)
	if len(matches) == 0 {
		return "", "", "", fmt.Errorf("failed to parse database name from %q according to pattern %q",
			db, validDBPattern.String())
	}
	return matches[1], matches[2], matches[3], nil
}
