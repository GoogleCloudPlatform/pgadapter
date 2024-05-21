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

// [START spanner_read_only_transaction]
import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func readOnlyTransaction(host string, port int, database string) error {
	ctx := context.Background()
	connString := fmt.Sprintf(
		"postgres://uid:pwd@%s:%d/%s?sslmode=disable",
		host, port, database)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Start a read-only transaction by supplying additional transaction options.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})

	albumsOrderedById, err := tx.Query(ctx, "SELECT singer_id, album_id, album_title FROM albums ORDER BY singer_id, album_id")
	defer albumsOrderedById.Close()
	if err != nil {
		return err
	}
	for albumsOrderedById.Next() {
		var singerId, albumId int64
		var title string
		err = albumsOrderedById.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		fmt.Printf("%v %v %v\n", singerId, albumId, title)
	}

	albumsOrderedTitle, err := tx.Query(ctx, "SELECT singer_id, album_id, album_title FROM albums ORDER BY album_title")
	defer albumsOrderedTitle.Close()
	if err != nil {
		return err
	}
	for albumsOrderedTitle.Next() {
		var singerId, albumId int64
		var title string
		err = albumsOrderedTitle.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		fmt.Printf("%v %v %v\n", singerId, albumId, title)
	}

	// End the read-only transaction by calling Commit().
	return tx.Commit(ctx)
}

// [END spanner_read_only_transaction]
