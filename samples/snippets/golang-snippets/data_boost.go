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

package golang_snippets

// [START spanner_data_boost]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func dataBoost(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// This enables Data Boost for all partitioned queries on this connection.
	_, _ = conn.Exec(ctx, "set spanner.data_boost_enabled=true")

	// Run a partitioned query. This query will use Data Boost.
	rows, err := conn.Query(ctx, "run partitioned query select singer_id, first_name, last_name from singers")
	defer rows.Close()
	if err != nil {
		return err
	}
	for rows.Next() {
		var singerId int64
		var firstName, lastName string
		err = rows.Scan(&singerId, &firstName, &lastName)
		if err != nil {
			return err
		}
		fmt.Printf("%v %v %v\n", singerId, firstName, lastName)
	}

	return nil
}

// [END spanner_data_boost]
