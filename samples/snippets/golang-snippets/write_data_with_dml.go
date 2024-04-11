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

// [START spanner_dml_getting_started_insert]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func writeDataWithDml(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	tag, err := conn.Exec(ctx,
		"INSERT INTO singers (singer_id, first_name, last_name) "+
			"VALUES ($1, $2, $3), ($4, $5, $6), "+
			"       ($7, $8, $9), ($10, $11, $12)",
		12, "Melissa", "Garcia",
		13, "Russel", "Morales",
		14, "Jacqueline", "Long",
		15, "Dylan", "Shaw")
	if err != nil {
		return err
	}
	fmt.Printf("%v records inserted\n", tag.RowsAffected())

	return nil
}

// [END spanner_dml_getting_started_insert]
