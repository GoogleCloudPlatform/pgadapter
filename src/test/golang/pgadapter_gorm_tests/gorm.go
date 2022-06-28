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
	"github.com/shopspring/decimal"
	"reflect"
	"time"

	"gorm.io/datatypes"
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

type AllTypes struct {
	ColBigint      *int64
	ColBool        *bool
	ColBytea       *[]byte
	ColFloat8      *float64
	ColInt         *int
	ColNumeric     *decimal.Decimal
	ColTimestamptz *time.Time
	ColDate        *datatypes.Date
	ColVarchar     *string `gorm:"primaryKey"`
}

//export TestFirst
func TestFirst(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()
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

//export TestQueryAllDataTypes
func TestQueryAllDataTypes(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()
	row := AllTypes{}
	db.First(&row)

	// First verify that nothing is null.
	if row.ColBigint == nil {
		return C.CString("ColBigint is null")
	}
	if row.ColBool == nil {
		return C.CString("ColBool is null")
	}
	if row.ColBytea == nil {
		return C.CString("ColBytea is null")
	}
	if row.ColFloat8 == nil {
		return C.CString("ColFloat8 is null")
	}
	if row.ColInt == nil {
		return C.CString("ColInt is null")
	}
	if row.ColNumeric == nil {
		return C.CString("ColNumeric is null")
	}
	if row.ColTimestamptz == nil {
		return C.CString("ColTimestamptz is null")
	}
	if row.ColDate == nil {
		return C.CString("ColDate is null")
	}
	if row.ColVarchar == nil {
		return C.CString("ColVarchar is null")
	}

	if g, w := *row.ColBigint, int64(1); g != w {
		return C.CString(fmt.Sprintf("ColBigint mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColBool, true; g != w {
		return C.CString(fmt.Sprintf("ColBool mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColBytea, []byte("test"); !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("ColBytea mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColFloat8, 3.14; g != w {
		return C.CString(fmt.Sprintf("ColFloat8 mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColInt, 100; g != w {
		return C.CString(fmt.Sprintf("ColInt mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColNumeric, decimal.RequireFromString("6.626"); !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("ColNumeric mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := row.ColTimestamptz.UTC(), parseTimestamp("2022-02-16T13:18:02.123456Z"); g != w {
		return C.CString(fmt.Sprintf("ColTimestamptz mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColDate, parseDate("2022-03-29"); !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("ColDate mismatch\nGot:  %v\nWant: %v", g, w))
	}
	if g, w := *row.ColVarchar, "test"; g != w {
		return C.CString(fmt.Sprintf("ColVarchar mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

//export TestQueryNullsAllDataTypes
func TestQueryNullsAllDataTypes(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()
	row := AllTypes{}
	db.First(&row)

	// Verify that all columns are null.
	if row.ColBigint != nil {
		return C.CString("ColBigint is not null")
	}
	if row.ColBool != nil {
		return C.CString("ColBool is not null")
	}
	if row.ColBytea != nil {
		return C.CString("ColBytea is not null")
	}
	if row.ColFloat8 != nil {
		return C.CString("ColFloat8 is not null")
	}
	if row.ColInt != nil {
		return C.CString("ColInt is not null")
	}
	if row.ColNumeric != nil {
		return C.CString("ColNumeric is not null")
	}
	if row.ColTimestamptz != nil {
		return C.CString("ColTimestamptz is not null")
	}
	if row.ColDate != nil {
		return C.CString("ColDate is not null")
	}
	if row.ColVarchar != nil {
		return C.CString("ColVarchar is not null")
	}

	return nil
}

//export TestInsertAllDataTypes
func TestInsertAllDataTypes(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()

	row := AllTypes{
		ColBigint:      int64Ref(100),
		ColBool:        boolRef(true),
		ColBytea:       bytesRef([]byte("test_bytes")),
		ColFloat8:      float64Ref(3.14),
		ColInt:         intRef(1),
		ColNumeric:     decimalRef(decimal.RequireFromString("6.626")),
		ColTimestamptz: timeRef(parseTimestamp("2022-03-24T07:39:10.123456789+01:00")),
		ColDate:        dateRef(parseDate("2022-04-02")),
		ColVarchar:     stringRef("test_string"),
	}
	res := db.Create(&row)
	if res.Error != nil {
		return C.CString(fmt.Sprintf("failed to execute insert statement: %v", res.Error))
	}
	if g, w := res.RowsAffected, int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

//export TestInsertNullsAllDataTypes
func TestInsertNullsAllDataTypes(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()

	// Only set the primary key value and leave all other values empty (null).
	row := AllTypes{ColBigint: int64Ref(100)}
	res := db.Create(&row)
	if res.Error != nil {
		return C.CString(fmt.Sprintf("failed to execute insert statement: %v", res.Error))
	}
	if g, w := res.RowsAffected, int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

//export TestUpdateAllDataTypes
func TestUpdateAllDataTypes(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()

	row := AllTypes{
		ColBigint:      int64Ref(100),
		ColBool:        boolRef(true),
		ColBytea:       bytesRef([]byte("test_bytes")),
		ColFloat8:      float64Ref(3.14),
		ColInt:         intRef(1),
		ColNumeric:     decimalRef(decimal.RequireFromString("6.626")),
		ColTimestamptz: timeRef(parseTimestamp("2022-03-24T07:39:10.123456789+01:00")),
		ColDate:        dateRef(parseDate("2022-04-02")),
		ColVarchar:     stringRef("test_string"),
	}
	res := db.Save(row)
	if res.Error != nil {
		return C.CString(fmt.Sprintf("failed to execute update statement: %v", res.Error))
	}
	if g, w := res.RowsAffected, int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

//export TestDelete
func TestDelete(connString string) *C.char {
	db, err := gorm.Open(postgres.Open(connString), &gorm.Config{})
	if err != nil {
		return C.CString(err.Error())
	}
	conn, err := db.DB()
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close()

	row := AllTypes{ColVarchar: stringRef("test_string")}
	res := db.Delete(row)
	if res.Error != nil {
		return C.CString(fmt.Sprintf("failed to execute delete statement: %v", res.Error))
	}
	if g, w := res.RowsAffected, int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

func int64Ref(val int64) *int64 {
	return &val
}

func boolRef(val bool) *bool {
	return &val
}

func bytesRef(val []byte) *[]byte {
	return &val
}

func float64Ref(val float64) *float64 {
	return &val
}

func intRef(val int) *int {
	return &val
}

func decimalRef(val decimal.Decimal) *decimal.Decimal {
	return &val
}

func timeRef(val time.Time) *time.Time {
	return &val
}

func dateRef(val datatypes.Date) *datatypes.Date {
	return &val
}

func stringRef(val string) *string {
	return &val
}

func parseTimestamp(ts string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, ts)
	return t.UTC()
}

// parseDate returns the given date string as a Date. The Date contains a Time instance at the given
// date with all time components set to zero in the local timezone.
func parseDate(ds string) datatypes.Date {
	date := datatypes.Date{}
	_, offset := time.Now().Zone()
	ts := parseTimestamp(ds + "T00:00:00Z")
	ts.Add(-time.Second * time.Duration(offset))
	_ = date.Scan(ts)

	return date
}
