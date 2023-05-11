package runners

import (
	"context"
	"google.golang.org/api/iterator"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
)

var rnd *rand.Rand

func RunClientLib(db, sql string, numExecutions int) ([]float64, error) {
	ctx := context.Background()
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Run one query to warm up.
	if _, err := executeClientLibQuery(ctx, client, sql); err != nil {
		return nil, err
	}

	runTimes := make([]float64, numExecutions)
	for n := 0; n < numExecutions; n++ {
		runTimes[n], err = executeClientLibQuery(ctx, client, sql)
		if err != nil {
			return nil, err
		}
	}

	return runTimes, nil
}

func executeClientLibQuery(ctx context.Context, client *spanner.Client, sql string) (float64, error) {
	start := time.Now()
	stmt := spanner.Statement{
		SQL:    sql,
		Params: map[string]interface{}{"p1": rnd.Int63n(100000)},
	}
	numNull := 0
	numNonNull := 0
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		var s spanner.NullString
		if err := row.Columns(&s); err != nil {
			return 0, err
		}
		if s.Valid {
			numNonNull++
		} else {
			numNull++
		}
	}
	end := float64(time.Since(start).Microseconds()) / 1e3
	return end, nil
}
