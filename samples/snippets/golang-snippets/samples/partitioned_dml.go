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

// [START spanner_partitioned_dml]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func PartitionedDML(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Enable Partitioned DML on this connection.
	if _, err := conn.Exec(ctx, "set spanner.autocommit_dml_mode='partitioned_non_atomic'"); err != nil {
		return err
	}
	// Back-fill a default value for the MarketingBudget column.
	tag, err := conn.Exec(ctx, "update albums set marketing_budget=0 where marketing_budget is null")
	if err != nil {
		return err
	}
	fmt.Printf("Updated at least %v albums\n", tag.RowsAffected())

	return nil
}

// [END spanner_partitioned_dml]
