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
	pgadapter "github.com/GoogleCloudPlatform/pgadapter/wrappers/golang"
	"github.com/jackc/pgx/v5"
)

// This test application automatically starts PGAdapter in a Docker container and connects to
// Cloud Spanner through PGAdapter using `pgx`.
//
// Run with `go run pgx_sample.go` to run the sample on the Cloud Spanner emulator.
//
// Run with `go run pgx_sample.go -emulator=false -project my-project -instance my-instance -database my-database` to run the sample on
// an existing Cloud Spanner database.
func main() {
	// TODO(developer): Change default to `false` if you do not want to default to using the Cloud Spanner emulator.
	emulator := flag.Bool("emulator", true, "Use the Cloud Spanner Emulator instead of a real Cloud Spanner instance")

	// TODO(developer): Uncomment if your environment does not already have default credentials set.
	// os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json")

	// TODO(developer): Replace defaults with your project, instance and database if you want to run the sample
	//                  without having to specify any command line arguments.
	project := flag.String("project", "my-project", "The Google Cloud project of the Cloud Spanner instance")
	instance := flag.String("instance", "my-instance", "The Cloud Spanner instance to connect to")
	database := flag.String("database", "my-database", "The Cloud Spanner database to connect to")
	flag.Parse()
	if *emulator {
		fmt.Printf("\nConnecting to projects/%s/instances/%s/databases/%s on the emulator\n\n", *project, *instance, *database)
	} else {
		fmt.Printf("\nConnecting to projects/%s/instances/%s/databases/%s on Cloud Spanner\n\n", *project, *instance, *database)
	}

	// Start PGAdapter as a child process.
	// PGAdapter will by default be started as a Java application if Java is available on this host.
	// Otherwise, it will fall back to starting PGAdapter in a Docker test container.
	ctx := context.Background()
	pg, err := pgadapter.Start(ctx, pgadapter.Config{Project: *project, Instance: *instance, ConnectToEmulator: *emulator})
	if err != nil {
		fmt.Printf("failed to start PGAdapter: %v\n", err)
		return
	}
	// Stop PGAdapter when this function returns.
	// This is not required, as the PGAdapter sub-process shuts down automatically when your
	// application shuts down. The PGAdapter sub-process is not guaranteed to shut down if
	// your application is killed or crashes.
	defer pg.Stop(ctx)

	// Get the TCP port that was assigned to PGAdapter.
	port, err := pg.GetHostPort()
	if err != nil {
		fmt.Printf("failed to start get TCP port for PGAdapter: %v\n", err)
		return
	}
	// Connect to Cloud Spanner through PGAdapter.
	connString := fmt.Sprintf("postgres://uid:pwd@localhost:%d/%s?sslmode=disable", port, *database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		fmt.Printf("failed to connect to PGAdapter: %v\n", err)
		return
	}
	defer conn.Close(ctx)

	// Execute a query on Cloud Spanner.
	var greeting string
	err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
	if err != nil {
		fmt.Printf("failed to query Cloud Spanner: %v\n", err)
		return
	}
	fmt.Printf("\nGreeting from Cloud Spanner PostgreSQL: %v\n\n", greeting)
}
