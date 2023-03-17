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

	"github.com/jackc/pgx/v5"
	"pgadapter-pgx-sample/pgadapter"
)

// This test application automatically starts PGAdapter in a Docker container and connects to
// Cloud Spanner through PGAdapter using `pgx`.
func main() {
	// TODO(developer): Uncomment if your environment does not already have default credentials set.
	// os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json")

	// TODO(developer): Replace defaults with your project, instance and database if you want to run the sample
	//                  without having to specify any command line arguments.
	project := flag.String("project", "my-project", "The Google Cloud project of the Cloud Spanner instance")
	instance := flag.String("instance", "my-instance", "The Cloud Spanner instance to connect to")
	database := flag.String("database", "my-database", "The Cloud Spanner database to connect to")
	flag.Parse()
	fmt.Printf("\nConnecting to projects/%s/instances/%s/databases/%s\n\n", *project, *instance, *database)

	// Start PGAdapter in a Docker container.
	port, cleanup, err := pgadapter.StartPGAdapter(context.Background(), *project, *instance)
	defer cleanup()
	if err != nil {
		fmt.Printf("failed to start PGAdapter: %v\n", err)
		return
	}

	// Connect to Cloud Spanner through PGAdapter.
	connString := fmt.Sprintf("postgres://uid:pwd@localhost:%d/%s?sslmode=disable", port, *database)
	ctx := context.Background()
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
