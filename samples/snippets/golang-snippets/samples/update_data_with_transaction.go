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

// [START spanner_dml_getting_started_update]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func WriteWithTransactionUsingDml(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Transfer marketing budget from one album to another. We do it in a
	// transaction to ensure that the transfer is atomic.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	const selectSql = "SELECT marketing_budget " +
		"from albums " +
		"WHERE singer_id = $1 and album_id = $2"
	// Get the marketing_budget of singer 2 / album 2.
	row := tx.QueryRow(ctx, selectSql, 2, 2)
	var budget2 int64
	if err := row.Scan(&budget2); err != nil {
		tx.Rollback(ctx)
		return err
	}
	const transfer = 20000
	// The transaction will only be committed if this condition still holds
	// at the time of commit. Otherwise, the transaction will be aborted.
	if budget2 >= transfer {
		// Get the marketing_budget of singer 1 / album 1.
		row := tx.QueryRow(ctx, selectSql, 1, 1)
		var budget1 int64
		if err := row.Scan(&budget1); err != nil {
			tx.Rollback(ctx)
			return err
		}
		// Transfer part of the marketing budget of Album 2 to Album 1.
		budget1 += transfer
		budget2 -= transfer
		const updateSql = "UPDATE albums " +
			"SET marketing_budget = $1 " +
			"WHERE singer_id = $2 and album_id = $3"
		// Start a DML batch and execute it as part of the current transaction.
		batch := &pgx.Batch{}
		batch.Queue(updateSql, budget1, 1, 1)
		batch.Queue(updateSql, budget2, 2, 2)
		br := tx.SendBatch(ctx, batch)
		_, err = br.Exec()
		if err := br.Close(); err != nil {
			tx.Rollback(ctx)
			return err
		}
	}
	// Commit the current transaction.
	tx.Commit(ctx)
	fmt.Println("Transferred marketing budget from Album 2 to Album 1")

	return nil
}

// [END spanner_dml_getting_started_update]
