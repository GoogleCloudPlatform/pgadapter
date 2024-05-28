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

// [START spanner_create_database]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func CreateTables(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Create two tables in one batch on Spanner.
	br := conn.SendBatch(ctx, &pgx.Batch{QueuedQueries: []*pgx.QueuedQuery{
		{SQL: "create table singers (" +
			"  singer_id   bigint primary key not null," +
			"  first_name  character varying(1024)," +
			"  last_name   character varying(1024)," +
			"  singer_info bytea," +
			"  full_name   character varying(2048) generated " +
			"  always as (first_name || ' ' || last_name) stored" +
			")"},
		{SQL: "create table albums (" +
			"  singer_id     bigint not null," +
			"  album_id      bigint not null," +
			"  album_title   character varying(1024)," +
			"  primary key (singer_id, album_id)" +
			") interleave in parent singers on delete cascade"},
	}})
	cmd, err := br.Exec()
	if err != nil {
		return err
	}
	if cmd.String() != "CREATE" {
		return fmt.Errorf("unexpected command tag: %v", cmd.String())
	}
	if err := br.Close(); err != nil {
		return err
	}
	fmt.Printf("Created Singers & Albums tables in database: [%s]\n", database)

	return nil
}

// [END spanner_create_database]
