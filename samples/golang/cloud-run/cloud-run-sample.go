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
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/jackc/pgx/v5"
)

var project, instance, database, qualifiedDatabaseName string

// This sample application can be built as a Docker image and deployed on Cloud Run.
// The Dockerfile defines a Docker image that is based on the PGAdapter Docker image.
// This Go sample application is added to that base image and the ENTRYPOINT of the
// Docker image is modified to start both PGAdapter and then this sample application.
func main() {
	// TODO(developer): Replace defaults with your project, instance and database if you want to run the sample
	//                  without having to specify any environment variables.
	project = getenv("SPANNER_PROJECT", "my-project")
	instance = getenv("SPANNER_INSTANCE", "my-instance")
	database = getenv("SPANNER_DATABASE", "my-database")
	qualifiedDatabaseName = fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	log.Printf("\nConnecting to %s\n\n", qualifiedDatabaseName)

	log.Print("starting server...")
	http.HandleFunc("/", handler)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	// Connect to Cloud Spanner through PGAdapter.
	// Use url.QueryEscape to URL encode the fully qualified database name. This will replace '/' with '%2F'.
	connString := fmt.Sprintf("postgres://uid:pwd@127.0.0.1:%d/%s?sslmode=disable", 5432, url.QueryEscape(qualifiedDatabaseName))
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		fmt.Fprintf(w, "failed to connect to PGAdapter: %v\n", err)
		return
	}
	defer conn.Close(ctx)

	// Execute a query on Cloud Spanner.
	var greeting string
	err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(w, "failed to query Cloud Spanner: %v\n", err)
		return
	}
	fmt.Fprintf(w, "\nGreeting from Cloud Spanner PostgreSQL: %v\n\n", greeting)
}

func getenv(key, def string) string {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	return value
}
