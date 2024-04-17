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

// [START spanner_statement_timeout]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func queryDataWithTimeout(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Set the statement timeout that should be used for all statements
	// on this connection to 5 seconds.
	// Supported time units are 's' (seconds), 'ms' (milliseconds),
	// 'us' (microseconds), and 'ns' (nanoseconds).
	if _, err := conn.Exec(ctx, "set statement_timeout='5s'"); err != nil {
		return err
	}
	rows, err := conn.Query(ctx,
		"SELECT singer_id, album_id, album_title "+
			"FROM albums "+
			"WHERE album_title in ("+
			"  SELECT first_name "+
			"  FROM singers "+
			"  WHERE last_name LIKE '%a%'"+
			"     OR last_name LIKE '%m%'"+
			")")
	defer rows.Close()
	if err != nil {
		return err
	}
	for rows.Next() {
		var singerId, albumId int64
		var title string
		err = rows.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		fmt.Printf("%v %v %v\n", singerId, albumId, title)
	}

	return rows.Err()
}

// [END spanner_statement_timeout]
