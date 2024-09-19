/*
Copyright 2024 Google LLC
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
	"os"

	pgadapter "github.com/GoogleCloudPlatform/pgadapter/wrappers/golang"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	msg, err := runSample()
	if err != nil {
		fmt.Printf("Failed to run migrations: %v", err)
		os.Exit(1)
	}
	fmt.Println(msg)
}

func runSample() (string, error) {
	ctx := context.Background()

	// Start a PGAdapter instance with the emulator embedded and connect to this.
	pg, err := pgadapter.Start(ctx, pgadapter.Config{
		Project:           "emulator-project",
		Instance:          "test-instance",
		ConnectToEmulator: true,
	})
	if err != nil {
		fmt.Printf("failed to start PGAdapter: %v\n", err)
		return "", err
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
		return "", err
	}

	// Connect to PGAdapter and execute the migrations.
	database := "test-database"

	// IMPORTANT: PGAdapter will only recognize the Golang migrate tool if you set this application name
	//            in the connection string!
	applicationName := "golang-migrate"

	connString := fmt.Sprintf("postgres://localhost:%d/%s?sslmode=disable&fallback_application_name=%s", port, database, applicationName)
	m, err := migrate.New("file://migrations", connString)
	if err != nil {
		return "", err
	}
	// Execute the 2 first migration steps.
	if err := m.Steps(2); err != nil {
		return "", err
	}
	// Execute all remaining migration steps.
	if err := m.Up(); err != nil {
		return "", err
	}
	return "Finished running migrations on Spanner", nil
}
