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

// [START spanner_transaction_and_statement_tag]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func tags(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	// Set the TRANSACTION_TAG session variable to set a transaction tag
	// for the current transaction.
	_, _ = tx.Exec(ctx, "set spanner.transaction_tag='example-tx-tag'")

	// Set the STATEMENT_TAG session variable to set the request tag
	// that should be included with the next SQL statement.
	_, _ = tx.Exec(ctx, "set spanner.statement_tag='query-marketing-budget'")

	row := tx.QueryRow(ctx, "select marketing_budget "+
		"from albums "+
		"where singer_id=$1 and album_id=$2", 1, 1)
	var budget int64
	if err := row.Scan(&budget); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Reduce the marketing budget by 10% if it is more than 1,000.
	if budget > 1000 {
		budget = int64(float64(budget) - float64(budget)*0.1)
		_, _ = tx.Exec(ctx, "set spanner.statement_tag='reduce-marketing-budget'")
		if _, err := tx.Exec(ctx, "update albums set marketing_budget=$1 where singer_id=$2 AND album_id=$3", budget, 1, 1); err != nil {
			tx.Rollback(ctx)
			return err
		}
	}
	// Commit the current transaction.
	tx.Commit(ctx)
	fmt.Println("Reduced marketing budget")

	return nil
}

// [END spanner_transaction_and_statement_tag]
