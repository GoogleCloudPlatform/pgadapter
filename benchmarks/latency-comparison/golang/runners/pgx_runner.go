package runners

import (
	"cloud.google.com/pgadapter-latency-benchmark/runners/pgadapter"
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func RunPgx(db, sql string, numOperations, numClients int, useUnixSocket, startPgAdapter bool) ([]float64, error) {
	ctx := context.Background()
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Start PGAdapter in a Docker container.
	project, instance, database, err := parseDatabaseName(db)
	if err != nil {
		return nil, err
	}
	var port int
	if startPgAdapter {
		var cleanup func()
		port, cleanup, err = pgadapter.StartPGAdapter(context.Background(), project, instance)
		defer cleanup()
		if err != nil {
			return nil, err
		}
	} else {
		port = 5432
	}

	// Connect to Cloud Spanner through PGAdapter.
	var connString string
	if useUnixSocket {
		connString = fmt.Sprintf("host=/tmp port=5432 database=%s", database)
	} else {
		connString = fmt.Sprintf("postgres://uid:pwd@localhost:%d/%s?sslmode=disable", port, database)
	}
	conns := make([]*pgx.Conn, numClients)
	for c := 0; c < numClients; c++ {
		conns[c], err = pgx.Connect(ctx, connString)
		if err != nil {
			return nil, err
		}
		defer conns[c].Close(ctx)
	}

	// Run one query to warm up.
	if _, err := executePgxQuery(ctx, conns[0], sql); err != nil {
		return nil, err
	}

	runTimes := make([]float64, numOperations*numClients)
	wg := sync.WaitGroup{}
	wg.Add(numClients)
	for c := 0; c < numClients; c++ {
		clientIndex := c
		go func() error {
			defer wg.Done()
			for n := 0; n < numOperations; n++ {
				runTimes[clientIndex*numOperations+n], err = executePgxQuery(ctx, conns[clientIndex], sql)
				if err != nil {
					return err
				}
			}
			return nil
		}()
	}
	wg.Wait()
	return runTimes, nil
}

func executePgxQuery(ctx context.Context, conn *pgx.Conn, sql string) (float64, error) {
	start := time.Now()

	var res *string
	err := conn.QueryRow(ctx, sql, rnd.Int63n(100000)).Scan(&res)
	if err != nil && err != pgx.ErrNoRows {
		return 0, err
	}
	numNull := 0
	numNonNull := 0
	if res == nil {
		numNonNull++
	} else {
		numNull++
	}
	end := float64(time.Since(start).Microseconds()) / 1e3
	return end, nil
}

var (
	validDBPattern = regexp.MustCompile("^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$")
)

func parseDatabaseName(db string) (project, instance, database string, err error) {
	matches := validDBPattern.FindStringSubmatch(db)
	if len(matches) == 0 {
		return "", "", "", fmt.Errorf("Failed to parse database name from %q according to pattern %q",
			db, validDBPattern.String())
	}
	return matches[1], matches[2], matches[3], nil
}
