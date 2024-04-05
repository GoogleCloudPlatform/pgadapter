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

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func createTables(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf("postgres://uid:pwd@localhost:%d/%s?sslmode=disable", port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Create two tables in one batch on Spanner.
	br := conn.SendBatch(ctx, &pgx.Batch{QueuedQueries: []*pgx.QueuedQuery{
		{SQL: `CREATE TABLE Singers (
			     SingerId   bigint NOT NULL,
			     FirstName  character varying(1024),
			     LastName   character varying(1024),
			     SingerInfo bytea,
			     FullName character varying(2048) GENERATED
			       ALWAYS AS (FirstName || ' ' || LastName) STORED,
			     PRIMARY KEY (SingerId)
               )`},
		{SQL: `CREATE TABLE Albums (
                 SingerId     bigint NOT NULL,
                 AlbumId      bigint NOT NULL,
                 AlbumTitle   character varying(1024),
                 PRIMARY KEY (SingerId, AlbumId)
               ) INTERLEAVE IN PARENT Singers ON DELETE CASCADE`},
	}})
	cmd, err := br.Exec()
	if err != nil {
		return err
	}
	if cmd.String() != "CREATE" {
		return fmt.Errorf("unexpected command tag: %v", cmd.String())
	}
	fmt.Printf("Created Singers & Albums tables in database: [%s]\n", database)

	return nil
}
