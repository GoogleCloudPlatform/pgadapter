// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "C"
import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
)

// This file defines tests that can be called from Java and that will connect to any PGAdapter
// instance that is defined in the connection string that is passed in to each of the test
// functions. The PGAdapter instance can be an in-process instance that is created and started by
// the Java test framework, and the Spanner database that PGAdapter is connected to can be a mock
// Spanner database or a real Spanner database.
// Test errors are returned as C strings.

// An empty main method is required to build a shard C lib.
func main() {
}

//export TestHelloWorld
func TestHelloWorld(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var greeting string
	err = conn.QueryRow(ctx, "select 'Hello world!' as hello").Scan(&greeting)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := greeting, "Hello world!"; g != w {
		return C.CString(fmt.Sprintf("greeting mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestSelect1
func TestSelect1(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var value int64
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&value)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := value, int64(1); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}
