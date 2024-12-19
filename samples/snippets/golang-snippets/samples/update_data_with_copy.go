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

// [START spanner_update_data]
import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
)

func UpdateDataWithCopy(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Enable non-atomic mode. This makes the COPY operation non-atomic,
	// and allows it to exceed the Spanner mutation limit.
	if _, err := conn.Exec(ctx,
		"set spanner.autocommit_dml_mode='partitioned_non_atomic'"); err != nil {
		return err
	}
	// Instruct PGAdapter to use insert-or-update for COPY statements.
	// This enables us to use COPY to update data.
	if _, err := conn.Exec(ctx, "set spanner.copy_upsert=true"); err != nil {
		return err
	}

	// Create a pipe that can be used to write the data manually that we want to copy.
	reader, writer := io.Pipe()
	// Write the data to the pipe using a separate goroutine. This allows us to stream the data
	// to the COPY operation row-by-row.
	go func() error {
		for _, record := range []string{"1\t1\t100000\n", "2\t2\t500000\n"} {
			if _, err := writer.Write([]byte(record)); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
		return nil
	}()
	tag, err := conn.PgConn().CopyFrom(ctx, reader, "COPY albums (singer_id, album_id, marketing_budget) FROM STDIN")
	if err != nil {
		return err
	}
	fmt.Printf("Updated %v albums\n", tag.RowsAffected())

	return nil
}

// [END spanner_update_data]
