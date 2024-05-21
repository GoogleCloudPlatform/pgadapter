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

// [START spanner_dml_batch]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func WriteDataWithDmlBatch(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	sql := "INSERT INTO singers (singer_id, first_name, last_name) " +
		"VALUES ($1, $2, $3)"
	batch := &pgx.Batch{}
	batch.Queue(sql, 16, "Sarah", "Wilson")
	batch.Queue(sql, 17, "Ethan", "Miller")
	batch.Queue(sql, 18, "Maya", "Patel")
	br := conn.SendBatch(ctx, batch)
	_, err = br.Exec()
	if err := br.Close(); err != nil {
		return err
	}

	if err != nil {
		return err
	}
	fmt.Printf("%v records inserted\n", batch.Len())

	return nil
}

// [END spanner_dml_batch]
