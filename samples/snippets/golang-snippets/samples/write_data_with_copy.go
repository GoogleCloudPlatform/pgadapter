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

// [START spanner_copy_from_stdin]
import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func WriteDataWithCopy(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
	// will succeed even if it exceeds Spanner's mutation limit per transaction.
	conn.Exec(ctx, "set spanner.autocommit_dml_mode='partitioned_non_atomic'")

	file, err := os.Open("samples/singers_data.txt")
	if err != nil {
		return err
	}
	tag, err := conn.PgConn().CopyFrom(ctx, file,
		"copy singers (singer_id, first_name, last_name) from stdin")
	if err != nil {
		return err
	}
	fmt.Printf("Copied %v singers\n", tag.RowsAffected())

	file, err = os.Open("samples/albums_data.txt")
	if err != nil {
		return err
	}
	tag, err = conn.PgConn().CopyFrom(ctx, file,
		"copy albums from stdin")
	if err != nil {
		return err
	}
	fmt.Printf("Copied %v albums\n", tag.RowsAffected())

	return nil
}

// [END spanner_copy_from_stdin]
