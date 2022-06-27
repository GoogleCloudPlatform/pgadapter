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
	"database/sql"
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// This file defines tests that can be called from Java and that will connect to any PGAdapter
// instance that is defined in the connection string that is passed in to each of the test
// functions. The PGAdapter instance can be an in-process instance that is created and started by
// the Java test framework, and the Spanner database that PGAdapter is connected to can be a mock
// Spanner database or a real Spanner database.
// Test errors are returned as C strings.

// An empty main method is required to build a shared C lib.
func main() {
}

type User struct {
	ID           uint
	Name         string
	Email        *string
	Age          uint8
	Birthday     *time.Time
	MemberNumber sql.NullString
	ActivatedAt  sql.NullTime
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

//export TestFirst
func TestFirst(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	user := User{}
	db.First(&user)

	if g, w := user.ID, uint(1); g != w {
		return C.CString(fmt.Sprintf("User ID mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := user.Name, "Some Name"; g != w {
		return C.CString(fmt.Sprintf("Name mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if user.Email != nil {
		if g, w := *user.Email, "user@example.com"; g != w {
			return C.CString(fmt.Sprintf("Email mismatch\nGot:  %v\nWant: %v", g, w))
		}
	} else {
		return C.CString("Email is null")
	}
	if g, w := user.Age, uint8(62); g != w {
		return C.CString(fmt.Sprintf("Age mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if user.Birthday != nil {
		if g, w := user.Birthday.UTC(), parseTimestamp("1960-06-27T16:44:10.123456Z"); g != w {
			return C.CString(fmt.Sprintf("Birthday mismatch\nGot:  %v\nWant: %v", g, w))
		}
	} else {
		return C.CString("Birthday is null")
	}
	if g, w := user.MemberNumber.String, "MN9999"; g != w {
		return C.CString(fmt.Sprintf("Member number mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := user.ActivatedAt.Time.UTC(), parseTimestamp("2021-01-04T10:00:00Z"); g != w {
		return C.CString(fmt.Sprintf("ActivatedAt mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := user.CreatedAt.UTC(), parseTimestamp("2000-01-01T00:00:00Z"); g != w {
		return C.CString(fmt.Sprintf("CreatedAt mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := user.UpdatedAt.UTC(), parseTimestamp("2022-05-22T12:13:14.123Z"); g != w {
		return C.CString(fmt.Sprintf("UpdatedAt mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

func parseTimestamp(ts string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, ts)
	return t.UTC()
}
