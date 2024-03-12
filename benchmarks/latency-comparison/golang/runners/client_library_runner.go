/*
Copyright 2023 Google LLC
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

package runners

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var rnd *rand.Rand
var m *sync.Mutex

func init() {
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	m = &sync.Mutex{}
}

func RunClientLib(db, sql string, readWrite bool, numOperations, numClients, wait int) ([]float64, error) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Run one query to warm up.
	if readWrite {
		if _, err := executeClientLibUpdate(ctx, client, sql); err != nil {
			return nil, err
		}
	} else {
		if _, err := executeClientLibQuery(ctx, client, sql); err != nil {
			return nil, err
		}
	}

	runTimes := make([]float64, numOperations*numClients)
	wg := sync.WaitGroup{}
	wg.Add(numClients)
	for c := 0; c < numClients; c++ {
		clientIndex := c
		go func() error {
			defer wg.Done()
			for n := 0; n < numOperations; n++ {
				randWait(wait)
				if readWrite {
					runTimes[clientIndex*numOperations+n], err = executeClientLibUpdate(ctx, client, sql)
				} else {
					runTimes[clientIndex*numOperations+n], err = executeClientLibQuery(ctx, client, sql)
				}
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

func executeClientLibQuery(ctx context.Context, client *spanner.Client, sql string) (float64, error) {
	start := time.Now()
	stmt := spanner.Statement{
		SQL:    sql,
		Params: map[string]interface{}{"p1": randId(100000)},
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

func executeClientLibUpdate(ctx context.Context, client *spanner.Client, sql string) (float64, error) {
	start := time.Now()
	stmt := spanner.Statement{
		SQL: sql,
		Params: map[string]interface{}{
			"p1": randString(),
			"p2": randId(100000),
		},
	}
	if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		_, err := transaction.Update(ctx, stmt)
		return err
	}); err != nil {
		return 0, err
	}
	end := float64(time.Since(start).Microseconds()) / 1e3
	return end, nil
}

func randWait(wait int) {
	m.Lock()
	sleep := rand.Intn(2 * wait)
	m.Unlock()
	time.Sleep(time.Duration(sleep) * time.Millisecond)
}

func randId(n int64) int64 {
	m.Lock()
	defer m.Unlock()
	return rnd.Int63n(n)
}

func randString() string {
	b := make([]byte, 64)
	m.Lock()
	defer m.Unlock()
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
