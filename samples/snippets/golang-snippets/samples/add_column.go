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

// [START spanner_add_column]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func AddColumn(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx,
		"ALTER TABLE albums "+
			"ADD COLUMN marketing_budget bigint")
	if err != nil {
		return err
	}
	fmt.Println("Added marketing_budget column")

	return nil
}

// [END spanner_add_column]
