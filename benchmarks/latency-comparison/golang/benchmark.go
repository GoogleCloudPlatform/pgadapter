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
	embedded := flag.Bool("embedded", false, "Starts an embedded PGAdapter container along with the benchmark. Setting this option will ignore any host or port settings for PGAdapter.\nNOTE: Running PGAdapter in a Docker container while the application runs on the host machine will add significant latency, as all communication between the application and PGAdapter will have to cross the Docker network bridge. You should only use this option for testing purposes, and not for actual performance benchmarking.")
	host := flag.String("host", "localhost", "The host name where PGAdapter is running. Only used if embedded=false.")
	port := flag.Int("port", 5432, "The port number where PGAdapter is running. Only used if embedded=false.")
	uds := flag.Bool("uds", false, "Run a benchmark using Unix Domain Socket in addition to TCP.")
	dir := flag.String("dir", "/tmp", "The directory where PGAdapter listens for Unix Domain Socket connections. Only used if embedded=false.")
	udsPort := flag.Int("udsport", 0, "The port number where PGAdapter is listening for Unix Domain Sockets. Only used if embedded=false.")
	flag.Parse()

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

	sql := "select col_varchar from latency_test where col_bigint=$1"
	fmt.Println()
	fmt.Println("Running benchmark with the following settings:")
	fmt.Printf("Database: %s\n", *db)
	fmt.Printf("Clients: %d\n", *numClients)
	fmt.Printf("Operations: %d\n", *numOperations)
	fmt.Println()

	fmt.Println("Running pgx v5 benchmark using TCP against PGAdapter")
	pgxTcpRunTimes, err := runners.RunPgx(*db, sql, *numOperations, *numClients, *host, *port, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Running pgx v4 benchmark using TCP against PGAdapter")
	pgxV4TcpRunTimes, err := runners.RunPgxV4(*db, sql, *numOperations, *numClients, *host, *port, false)
	if err != nil {
		panic(err)
	}

	var pgxUdsRunTimes []float64
	var pgxV4UdsRunTimes []float64
	if *uds {
		fmt.Println("Running pgx v5 benchmark using Unix Domain Socket against PGAdapter")
		pgxUdsRunTimes, err = runners.RunPgx(*db, sql, *numOperations, *numClients, *dir, *udsPort, true)
		if err != nil {
			panic(err)
		}
		fmt.Println("Running pgx v4 benchmark using Unix Domain Socket against PGAdapter")
		pgxV4UdsRunTimes, err = runners.RunPgxV4(*db, sql, *numOperations, *numClients, *dir, *udsPort, true)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Running native Cloud Spanner client library benchmark")
	clientLibRunTimes, err := runners.RunClientLib(*db, sql, *numOperations, *numClients)
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
