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

// [START spanner_update_data]
import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func updateDataWithCopy(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	//
	//            # COPY uses mutations to insert or update existing data in Spanner.
	//            with cur.copy("COPY albums (singer_id, album_id, marketing_budget) "
	//                          "FROM STDIN") as copy:
	//                copy.write_row((1, 1, 100000))
	//                copy.write_row((2, 2, 500000))
	//            print("Updated %d albums" % cur.rowcount)

	// Enable non-atomic mode. This makes the COPY operation non-atomic,
	// and allows it to exceed the Spanner mutation limit.
	if _, err := conn.Exec(ctx,
		"set spanner.autocommit_dml_mode='partitioned_non_atomic"); err != nil {
		return err
	}
	// Instruct PGAdapter to use insert-or-update for COPY statements.
	// This enables us to use COPY to update data.
	if _, err := conn.Exec(ctx, "set spanner.copy_upsert=true"); err != nil {
		return err
	}

	file, err := os.Open("singers_data.txt")
	if err != nil {
		return err
	}
	tag, err := conn.PgConn().CopyFrom(ctx, file,
		"copy singers (singer_id, first_name, last_name) from stdin")
	if err != nil {
		return err
	}
	fmt.Printf("Copied %v singers\n", tag.RowsAffected())

	file, err = os.Open("albums_data.txt")
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

// [END spanner_update_data]
