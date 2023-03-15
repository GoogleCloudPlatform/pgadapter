/**
 * Copyright 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	// register greet function to handle all requests
	mux := http.NewServeMux()
	mux.HandleFunc("/", greet)

	// use PORT environment variable, or default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// start the web server on port and accept requests
	log.Printf("Server listening on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// greet connects to PGAdapter and responds to the request with a greeting from Cloud Spanner.
func greet(w http.ResponseWriter, r *http.Request) {
	log.Printf("Serving request: %s", r.URL.Path)
	host, _ := os.Hostname()
	fmt.Fprintf(w, "Hostname: %s\n", host)

	conn, err := pgx.Connect(context.Background(), "postgres://uid:pwd@127.0.0.1:5432/?sslmode=disable")
	if err != nil {
		fmt.Fprintf(w, "Unable to connect to database: %v\n", err)
		return
	}
	defer conn.Close(context.Background())

	var greeting string
	err = conn.QueryRow(context.Background(), "select 'Greeting from PGAdapter!'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(w, "QueryRow failed: %v\n", err)
		return
	}
	fmt.Fprintln(w, greeting)
}
