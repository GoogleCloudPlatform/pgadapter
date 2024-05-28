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

package samples

// [START spanner_create_connection]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func CreateConnection(host string, port int, database string) error {
	ctx := context.Background()
	// Connect to Cloud Spanner using pgx through PGAdapter.
	// 'sslmode=disable' is optional, but adding it reduces the connection time,
	// as pgx will then skip first trying to create an SSL connection.
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	row := conn.QueryRow(ctx, "select 'Hello world!' as hello")
	var msg string
	if err := row.Scan(&msg); err != nil {
		return err
	}
	fmt.Printf("Greeting from Cloud Spanner PostgreSQL: %s\n", msg)

	return nil
}

// [END spanner_create_connection]
