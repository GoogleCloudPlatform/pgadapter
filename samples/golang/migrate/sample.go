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
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	m, err := migrate.New(
		"file://migrations",
		"postgres://localhost:9999/database?sslmode=disable&fallback_application_name=golang-migrate")
	if err != nil {
		fmt.Printf("failed to migrate: %v\n", err)
		os.Exit(1)
	}
	// Execute the 2 first migration steps.
	if err := m.Steps(2); err != nil {
		fmt.Printf("failed to execute 2 migrate steps: %v\n", err)
		os.Exit(1)
	}
	// Execute all remaining migration steps.
	if err := m.Up(); err != nil {
		fmt.Printf("failed to execute all migrate steps: %v\n", err)
		os.Exit(1)
	}
}
