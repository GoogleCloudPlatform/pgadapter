/*
Copyright 2023 Google LLC
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"

	"cloud.google.com/pgadapter-latency-benchmark/runners"
	"cloud.google.com/pgadapter-latency-benchmark/runners/pgadapter"
	"github.com/montanaflynn/stats"
)

func main() {
	projectId, _ := os.LookupEnv("GOOGLE_CLOUD_PROJECT")
	if projectId == "" {
		projectId = "my-project"
	}
	instanceId, _ := os.LookupEnv("SPANNER_INSTANCE")
	if instanceId == "" {
		instanceId = "my-instance"
	}
	databaseId, _ := os.LookupEnv("SPANNER_DATABASE")
	if databaseId == "" {
		databaseId = "my-database"
	}

	db := flag.String("database", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId), "The database to use for benchmarking.")
	numClients := flag.Int("clients", 16, "The number of clients that will be executing queries in parallel.")
	numOperations := flag.Int("operations", 1000, "The number of operations that that each client will run. The total number of operations is clients*operations.")
	transaction := flag.String("transaction", "READ_ONLY", "The transaction type to execute. Must be either READ_ONLY or READ_WRITE.")
	wait := flag.Int("wait", 0, "The wait time in milliseconds between each query that is executed by each client. Defaults to 0. Set this to for example 1000 to have each client execute 1 query per second.")
	embedded := flag.Bool("embedded", false, "Starts an embedded PGAdapter container along with the benchmark. Setting this option will ignore any host or port settings for PGAdapter.\nNOTE: Running PGAdapter in a Docker container while the application runs on the host machine will add significant latency, as all communication between the application and PGAdapter will have to cross the Docker network bridge. You should only use this option for testing purposes, and not for actual performance benchmarking.")
	host := flag.String("host", "localhost", "The host name where PGAdapter is running. Only used if embedded=false.")
	port := flag.Int("port", 5432, "The port number where PGAdapter is running. Only used if embedded=false.")
	uds := flag.Bool("uds", false, "Run a benchmark using Unix Domain Socket in addition to TCP.")
	dir := flag.String("dir", "/tmp", "The directory where PGAdapter listens for Unix Domain Socket connections. Only used if embedded=false.")
	udsPort := flag.Int("udsport", 0, "The port number where PGAdapter is listening for Unix Domain Sockets. Only used if embedded=false.")
	warmupIterations := flag.Int("warmup", 60*1000/5, "The number of warmup iterations to run on PGAdapter before executing the actual benchmark.")
	flag.Parse()
	var readWrite bool
	if strings.EqualFold(*transaction, "READ_WRITE") {
		readWrite = true
	} else if strings.EqualFold(*transaction, "READ_ONLY") {
		readWrite = false
	} else {
		fmt.Printf("Invalid value for transaction type: %s\n", *transaction)
		fmt.Println("Defaulting to READ_ONLY transaction type.")
		*transaction = "READ_ONLY"
	}

	// Start PGAdapter in a Docker container.
	project, instance, _, err := parseDatabaseName(*db)
	if *embedded {
		var cleanup func()
		*port, cleanup, err = pgadapter.StartPGAdapter(context.Background(), project, instance)
		*host = "localhost"
		*dir = "/tmp"
		*udsPort = 5432
		if err != nil {
			panic(err)
		}
		defer cleanup()
	} else {
		if *udsPort == 0 {
			*udsPort = *port
		}
	}

	querySql := "select col_varchar from latency_test where col_bigint=$1"
	updateSql := "update latency_test set col_varchar=$1 where col_bigint=$2"
	fmt.Println()
	fmt.Println("Running benchmark with the following settings:")
	fmt.Printf("Database: %s\n", *db)
	fmt.Printf("Clients: %d\n", *numClients)
	fmt.Printf("Operations: %d\n", *numOperations)
	fmt.Printf("Transaction type: %s\n", *transaction)
	fmt.Printf("Wait between queries: %dms\n", *wait)
	fmt.Println()

	var sql string
	if readWrite {
		sql = updateSql
	} else {
		sql = querySql
	}
	if *warmupIterations > 0 {
		fmt.Println("Running warmup script against PGAdapter")
		warmupClients := runtime.NumCPU()
		_, err = runners.RunPgx(*db, sql, readWrite, *warmupIterations, warmupClients, 0, *host, *port, false)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Running pgx v5 benchmark using TCP against PGAdapter")
	pgxTcpRunTimes, err := runners.RunPgx(*db, sql, readWrite, *numOperations, *numClients, *wait, *host, *port, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using TCP against PGAdapter")
	pgxV4TcpRunTimes, err := runners.RunPgxV4(*db, sql, readWrite, *numOperations, *numClients, *wait, *host, *port, false)
	if err != nil {
		panic(err)
	}

	var pgxUdsRunTimes []float64
	var pgxV4UdsRunTimes []float64
	if *uds {
		fmt.Println("Running pgx v5 benchmark using Unix Domain Socket against PGAdapter")
		pgxUdsRunTimes, err = runners.RunPgx(*db, sql, readWrite, *numOperations, *numClients, *wait, *dir, *udsPort, true)
		if err != nil {
			panic(err)
		}
		fmt.Println("Running pgx v4 benchmark using Unix Domain Socket against PGAdapter")
		pgxV4UdsRunTimes, err = runners.RunPgxV4(*db, sql, readWrite, *numOperations, *numClients, *wait, *dir, *udsPort, true)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Running native Cloud Spanner client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(*db, sql, readWrite, *numOperations, *numClients, *wait)
	if err != nil {
		panic(err)
	}

	printReport("pgx v5 with PGAdapter over TCP", *numClients, pgxTcpRunTimes)
	printReport("pgx v4 with PGAdapter over TCP", *numClients, pgxV4TcpRunTimes)
	if *uds {
		printReport("pgx v5 with PGAdapter over UDS", *numClients, pgxUdsRunTimes)
		printReport("pgx v4 with PGAdapter over UDS", *numClients, pgxV4UdsRunTimes)
	}
	printReport("Cloud Spanner native Go Client Library", *numClients, clientLibRunTimes)
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
	p99, err := stats.Percentile(runTimes, 99.0)
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
	fmt.Printf("p99: %fms\n", p99)
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
