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

// [START spanner_ddl_batch]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func ddlBatch(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Executing multiple DDL statements as one batch is
	// more efficient than executing each statement
	// individually.
	br := conn.SendBatch(ctx, &pgx.Batch{QueuedQueries: []*pgx.QueuedQuery{
		{SQL: "CREATE TABLE venues (" +
			"  venue_id    bigint not null primary key," +
			"  name        varchar(1024)," +
			"  description jsonb" +
			")"},
		{SQL: "CREATE TABLE concerts (" +
			"  concert_id bigint not null primary key ," +
			"  venue_id   bigint not null," +
			"  singer_id  bigint not null," +
			"  start_time timestamptz," +
			"  end_time   timestamptz," +
			"  constraint fk_concerts_venues foreign key" +
			"    (venue_id) references venues (venue_id)," +
			"  constraint fk_concerts_singers foreign key" +
			"    (singer_id) references singers (singer_id)" +
			")"},
	}})
	if _, err := br.Exec(); err != nil {
		return err
	}
	if err := br.Close(); err != nil {
		return err
	}
	fmt.Println("Added venues and concerts tables")

	return nil
}

// [END spanner_ddl_batch]
