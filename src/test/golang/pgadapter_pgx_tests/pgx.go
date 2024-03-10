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
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
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

//export TestShowApplicationName
func TestShowApplicationName(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var value string
	err = conn.QueryRow(ctx, "show application_name").Scan(&value)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := value, "pgx"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestQueryWithParameter
func TestQueryWithParameter(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var value string
	err = conn.QueryRow(ctx, "SELECT * FROM FOO WHERE BAR=$1", "baz").Scan(&value)
	if err != nil {
		return C.CString(fmt.Sprintf("Failed to execute query: %v", err.Error()))
	}
	if g, w := value, "baz"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestQueryAllDataTypes
func TestQueryAllDataTypes(connString string, oid, format int16) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var bigintValue int64
	var boolValue bool
	var byteaValue []byte
	var float4Value float32
	var float8Value float64
	var intValue int
	var numericValue pgtype.Numeric // pgx by default maps numeric to string
	var timestamptzValue time.Time
	var dateValue time.Time
	var varcharValue string
	var jsonbValue string

	var row pgx.Row
	if oid != 0 {
		formats := make(pgx.QueryResultFormatsByOID)
		for _, o := range []uint32{
			pgtype.Int8OID, pgtype.BoolOID, pgtype.ByteaOID, pgtype.Float4OID, pgtype.Float8OID,
			pgtype.Int4OID, pgtype.NumericOID, pgtype.TimestamptzOID, pgtype.DateOID,
			pgtype.VarcharOID, pgtype.JSONBOID, pgtype.Int8ArrayOID, pgtype.BoolArrayOID,
			pgtype.ByteaArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.Int4ArrayOID,
			pgtype.NumericArrayOID, pgtype.TimestamptzArrayOID, pgtype.DateArrayOID,
			pgtype.VarcharArrayOID, pgtype.JSONBArrayOID} {
			formats[o] = conn.ConnInfo().ResultFormatCodeForOID(o)
		}
		formats[uint32(oid)] = format
		row = conn.QueryRow(ctx, "SELECT col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, col_array_bigint, col_array_bool, col_array_bytea, col_array_float4, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb FROM all_types WHERE col_bigint=1", formats)
	} else {
		row = conn.QueryRow(ctx, "SELECT col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, col_array_bigint, col_array_bool, col_array_bytea, col_array_float4, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb FROM all_types WHERE col_bigint=1")
	}
	var arrayBigint, arrayBool, arrayBytea, arrayFloat4, arrayFloat8, arrayInt, arrayNumeric, arrayTimestamptz, arrayDate, arrayVarchar, arrayJsonb interface{}
	err = row.Scan(
		&bigintValue,
		&boolValue,
		&byteaValue,
		&float4Value,
		&float8Value,
		&intValue,
		&numericValue,
		&timestamptzValue,
		&dateValue,
		&varcharValue,
		&jsonbValue,
		&arrayBigint,
		&arrayBool,
		&arrayBytea,
		&arrayFloat4,
		&arrayFloat8,
		&arrayInt,
		&arrayNumeric,
		&arrayTimestamptz,
		&arrayDate,
		&arrayVarchar,
		&arrayJsonb,
	)
	if err != nil {
		return C.CString(fmt.Sprintf("Failed to execute query: %v", err.Error()))
	}
	if g, w := bigintValue, int64(1); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := boolValue, true; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := byteaValue, []byte("test"); !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := float4Value, float32(3.14); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := float8Value, 3.14; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := intValue, 100; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	var wantNumericValue pgtype.Numeric
	_ = wantNumericValue.Scan("6.626")
	if g, w := numericValue, wantNumericValue; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantDateValue, _ := time.Parse("2006-01-02", "2022-03-29")
	if g, w := dateValue, wantDateValue; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantTimestamptzValue, _ := time.Parse(time.RFC3339Nano, "2022-02-16T13:18:02.123456+00:00")
	if g, w := timestamptzValue.UTC().String(), wantTimestamptzValue.UTC().String(); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := varcharValue, "test"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := jsonbValue, "{\"key\": \"value\"}"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantInt8Array := pgtype.Int8Array{
		Elements:   []pgtype.Int8{{1, pgtype.Present}, {0, pgtype.Null}, {2, pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayBigint, wantInt8Array; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantBoolArray := pgtype.BoolArray{
		Elements:   []pgtype.Bool{{true, pgtype.Present}, {false, pgtype.Null}, {false, pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayBool, wantBoolArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantByteaArray := pgtype.ByteaArray{
		Elements:   []pgtype.Bytea{{[]byte("bytes1"), pgtype.Present}, {nil, pgtype.Null}, {[]byte("bytes2"), pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayBytea, wantByteaArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantFloat8Array := pgtype.Float8Array{
		Elements:   []pgtype.Float8{{3.14, pgtype.Present}, {0, pgtype.Null}, {-99.99, pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayFloat8, wantFloat8Array; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	// int (int4) values are automatically converted to bigint (int8) values in Cloud Spanner.
	wantInt4Array := pgtype.Int8Array{
		Elements:   []pgtype.Int8{{-100, pgtype.Present}, {0, pgtype.Null}, {-200, pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayInt, wantInt4Array; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	var wantNumericValue1 pgtype.Numeric
	_ = wantNumericValue1.Scan("6.626")
	var wantNumericValue2 pgtype.Numeric
	_ = wantNumericValue2.Scan("-3.14")
	wantNumericArray := pgtype.NumericArray{
		Elements:   []pgtype.Numeric{wantNumericValue1, {Status: pgtype.Null}, wantNumericValue2},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayNumeric, wantNumericArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantDateValue1, _ := time.Parse("2006-01-02", "2023-02-20")
	wantDateValue2, _ := time.Parse("2006-01-02", "2000-01-01")
	wantDateArray := pgtype.DateArray{
		Elements:   []pgtype.Date{{wantDateValue1, pgtype.Present, 0}, {time.Time{}, pgtype.Null, 0}, {wantDateValue2, pgtype.Present, 0}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayDate, wantDateArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantTimestamptzValue1, _ := time.Parse(time.RFC3339Nano, "2022-02-16T16:18:02.123456+00:00")
	wantTimestamptzValue2, _ := time.Parse(time.RFC3339Nano, "2000-01-01T00:00:00+00:00")
	wantTimestamptzArray := pgtype.TimestamptzArray{
		Elements:   []pgtype.Timestamptz{{wantTimestamptzValue1, pgtype.Present, 0}, {time.Time{}, pgtype.Null, 0}, {wantTimestamptzValue2, pgtype.Present, 0}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	gotVal1 := arrayTimestamptz.(pgtype.TimestamptzArray).Elements[0].Time.UTC().String()
	wantVal1 := wantTimestamptzArray.Elements[0].Time.UTC().String()
	gotVal2 := arrayTimestamptz.(pgtype.TimestamptzArray).Elements[1].Time.UTC().String()
	wantVal2 := wantTimestamptzArray.Elements[1].Time.UTC().String()
	if g, w := gotVal1, wantVal1; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := gotVal2, wantVal2; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantVarcharArray := pgtype.VarcharArray{
		Elements:   []pgtype.Varchar{{"string1", pgtype.Present}, {"", pgtype.Null}, {"string2", pgtype.Present}},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayVarchar, wantVarcharArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantJsonb1 := pgtype.JSONB{}
	wantJsonb1.Scan("{\"key\": \"value1\"}")
	wantJsonb2 := pgtype.JSONB{}
	wantJsonb2.Scan("{\"key\": \"value2\"}")
	wantJsonbArray := pgtype.JSONBArray{
		Elements:   []pgtype.JSONB{wantJsonb1, {nil, pgtype.Null}, wantJsonb2},
		Dimensions: []pgtype.ArrayDimension{{3, 1}},
		Status:     pgtype.Present}
	if g, w := arrayJsonb, wantJsonbArray; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestInsertAllDataTypes
func TestInsertAllDataTypes(connString string, float4Oid int) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	insertSql := "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, " +
		"col_array_bigint, col_array_bool, col_array_bytea, col_array_float4, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)"
	numeric := pgtype.Numeric{}
	_ = numeric.Set("6.626")
	numeric1 := pgtype.Numeric{}
	_ = numeric1.Set("-6.626")
	numeric2 := pgtype.Numeric{}
	_ = numeric2.Set("3.14")
	timestamptz, _ := time.Parse(time.RFC3339Nano, "2022-03-24T07:39:10.123456789+01:00")
	timestamptz2, _ := time.Parse(time.RFC3339Nano, "2000-01-01T00:00:00Z")
	var tag pgconn.CommandTag
	date := pgtype.Date{}
	_ = date.Set("2022-04-02")
	date2 := pgtype.Date{}
	_ = date2.Set("1970-01-01")

	var float4Value interface{}
	var float4Array interface{}
	if float4Oid == pgtype.Float4OID {
		float4Value = float32(3.14)
		float4Array = pgtype.Float4Array{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Float4{{Float: float32(3.14), Status: pgtype.Present}, {Status: pgtype.Null}, {Float: float32(6.626), Status: pgtype.Present}}}
	} else {
		float4Value = float64(float32(3.14))
		float4Array = pgtype.Float8Array{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Float8{{Float: float64(float32(3.14)), Status: pgtype.Present}, {Status: pgtype.Null}, {Float: float64(float32(6.626)), Status: pgtype.Present}}}
	}

	tag, err = conn.Exec(ctx, insertSql, 100, true, []byte("test_bytes"), float4Value, 3.14, 1, numeric, timestamptz, date, "test_string", "{\"key\": \"value\"}",
		pgtype.Int8Array{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Int8{{Int: 100, Status: pgtype.Present}, {Status: pgtype.Null}, {Int: 200, Status: pgtype.Present}}},
		pgtype.BoolArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Bool{{Bool: true, Status: pgtype.Present}, {Status: pgtype.Null}, {Bool: false, Status: pgtype.Present}}},
		[][]byte{[]byte("bytes1"), nil, []byte("bytes2")},
		float4Array,
		pgtype.Float8Array{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Float8{{Float: 3.14, Status: pgtype.Present}, {Status: pgtype.Null}, {Float: 6.626, Status: pgtype.Present}}},
		pgtype.Int8Array{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Int8{{Int: -1, Status: pgtype.Present}, {Status: pgtype.Null}, {Int: -2, Status: pgtype.Present}}},
		pgtype.NumericArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Numeric{numeric1, {Status: pgtype.Null}, numeric2}},
		pgtype.TimestamptzArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Timestamptz{{Time: timestamptz, Status: pgtype.Present}, {Status: pgtype.Null}, {Time: timestamptz2, Status: pgtype.Present}}},
		pgtype.DateArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Date{date, {Status: pgtype.Null}, date2}},
		pgtype.VarcharArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.Varchar{{String: "string1", Status: pgtype.Present}, {Status: pgtype.Null}, {String: "string2", Status: pgtype.Present}}},
		pgtype.JSONBArray{Dimensions: []pgtype.ArrayDimension{{3, 1}}, Status: pgtype.Present, Elements: []pgtype.JSONB{{Bytes: []byte("{\"key\": \"value1\"}"), Status: pgtype.Present}, {Status: pgtype.Null}, {Bytes: []byte("{\"key\": \"value2\"}"), Status: pgtype.Present}}},
	)
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute insert statement: %v", err))
	}
	if !tag.Insert() {
		return C.CString("statement was not recognized as an insert")
	}
	if g, w := tag.RowsAffected(), int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch:\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestInsertNullsAllDataTypes
func TestInsertNullsAllDataTypes(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	var tag pgconn.CommandTag
	sql := "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	tag, err = conn.Exec(ctx, sql, int64(100), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute insert statement: %v", err))
	}
	if !tag.Insert() {
		return C.CString("statement was not recognized as an insert")
	}
	if g, w := tag.RowsAffected(), int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch:\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestInsertAllDataTypesReturning
func TestInsertAllDataTypesReturning(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	sql := "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) returning *"
	numeric := pgtype.Numeric{}
	_ = numeric.Set("6.626")
	timestamptz, _ := time.Parse(time.RFC3339Nano, "2022-03-24T07:39:10.123456789+01:00")
	date := pgtype.Date{}
	_ = date.Set("2022-04-02")
	var row pgx.Row
	if strings.Contains(connString, "prefer_simple_protocol=true") {
		// Simple mode will format the date as '2022-04-02 00:00:00Z', which is not supported by the
		// backend yet.
		row = conn.QueryRow(ctx, sql, 100, true, []byte("test_bytes"), float32(3.14), 3.14, 1, numeric, timestamptz, "2022-04-02", "test_string", "{\"key\": \"value\"}")
	} else {
		row = conn.QueryRow(ctx, sql, 100, true, []byte("test_bytes"), float32(3.14), 3.14, 1, numeric, timestamptz, date, "test_string", "{\"key\": \"value\"}")
	}
	var bigintValue int64
	var boolValue bool
	var byteaValue []byte
	var float4Value float32
	var float8Value float64
	var intValue int
	var numericValue pgtype.Numeric // pgx by default maps numeric to string
	var timestamptzValue time.Time
	var dateValue time.Time
	var varcharValue string
	var jsonbValue string
	var arrayBigint, arrayBool, arrayBytea, arrayFloat4, arrayFloat8, arrayInt, arrayNumeric, arrayTimestamptz, arrayDate, arrayVarchar, arrayJsonb interface{}

	err = row.Scan(
		&bigintValue,
		&boolValue,
		&byteaValue,
		&float4Value,
		&float8Value,
		&intValue,
		&numericValue,
		&timestamptzValue,
		&dateValue,
		&varcharValue,
		&jsonbValue,
		&arrayBigint,
		&arrayBool,
		&arrayBytea,
		&arrayFloat4,
		&arrayFloat8,
		&arrayInt,
		&arrayNumeric,
		&arrayTimestamptz,
		&arrayDate,
		&arrayVarchar,
		&arrayJsonb,
	)
	if err != nil {
		return C.CString(fmt.Sprintf("Failed to execute insert: %v", err.Error()))
	}
	if g, w := bigintValue, int64(1); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := boolValue, true; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := byteaValue, []byte("test"); !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := float4Value, float32(3.14); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := float8Value, 3.14; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := intValue, 100; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	var wantNumericValue pgtype.Numeric
	_ = wantNumericValue.Scan("6.626")
	if g, w := numericValue, wantNumericValue; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	wantDateValue, _ := time.Parse("2006-01-02", "2022-03-29")
	if g, w := dateValue, wantDateValue; !reflect.DeepEqual(g, w) {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	// Encoding the timestamp values as a parameter will truncate it to microsecond precision.
	wantTimestamptzValue, _ := time.Parse(time.RFC3339Nano, "2022-02-16T13:18:02.123456+00:00")
	if g, w := timestamptzValue.UTC().String(), wantTimestamptzValue.UTC().String(); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := varcharValue, "test"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}
	if g, w := jsonbValue, "{\"key\": \"value\"}"; g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestUpdateAllDataTypes
func TestUpdateAllDataTypes(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	sql := "UPDATE \"all_types\" SET \"col_bigint\"=$1,\"col_bool\"=$2,\"col_bytea\"=$3,\"col_float8\"=$4,\"col_int\"=$5,\"col_numeric\"=$6,\"col_timestamptz\"=$7,\"col_date\"=$8,\"col_varchar\"=$9,\"col_jsonb\"=$10 WHERE \"col_varchar\" = $11"
	numeric := pgtype.Numeric{}
	_ = numeric.Set("6.626")
	timestamptz, _ := time.Parse(time.RFC3339Nano, "2022-03-24T07:39:10.123456789+01:00")
	var tag pgconn.CommandTag
	date := pgtype.Date{}
	_ = date.Set("2022-04-02")
	if strings.Contains(connString, "prefer_simple_protocol=true") {
		// Simple mode will format the date as '2022-04-02 00:00:00Z', which is not supported by the
		// backend yet.
		tag, err = conn.Exec(ctx, sql, 100, true, []byte("test_bytes"), 3.14, 1, numeric, timestamptz, "2022-04-02", "test_string", "{\"key\": \"value\"}", "test")
	} else {
		tag, err = conn.Exec(ctx, sql, 100, true, []byte("test_bytes"), 3.14, 1, numeric, timestamptz, date, "test_string", "{\"key\": \"value\"}", "test")
	}
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute update statement: %v", err))
	}
	if !tag.Update() {
		return C.CString("statement was not recognized as an update")
	}
	if g, w := tag.RowsAffected(), int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch:\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestPrepareStatement
func TestPrepareStatement(connString string, float4Oid int) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	sql := "UPDATE all_types SET col_int=$1, col_bool=$2, col_bytea=$3, col_float4=$4, col_float8=$5, " +
		"col_numeric=$6, col_timestamptz=$7, col_date=$8, col_varchar=$9, col_jsonb=$10 WHERE col_bigint=$11"
	sd, err := conn.Prepare(ctx, "update_all_types", sql)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := len(sd.ParamOIDs), 11; g != w {
		return C.CString(fmt.Sprintf("param type count mismatch:\n Got: %v\nWant: %v", g, w))
	}
	wantParamTypes := []int{
		pgtype.Int8OID,
		pgtype.BoolOID,
		pgtype.ByteaOID,
		float4Oid,
		pgtype.Float8OID,
		pgtype.NumericOID,
		pgtype.TimestamptzOID,
		pgtype.DateOID,
		pgtype.VarcharOID,
		pgtype.JSONBOID,
		pgtype.Int8OID,
	}
	for i, tp := range wantParamTypes {
		if g, w := sd.ParamOIDs[i], uint32(tp); g != w {
			return C.CString(fmt.Sprintf("param type mismatch for param[%v]:\n Got: %v\nWant: %v", i, g, w))
		}
	}
	if g, w := len(sd.Fields), 0; g != w {
		return C.CString(fmt.Sprintf("field count mismatch:\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestPrepareSelectStatement
func TestPrepareSelectStatement(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	sql := "SELECT col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb " +
		"FROM all_types " +
		"WHERE col_int=$1 AND col_bool=$2 AND col_bytea=$3 AND col_float8=$4 AND " +
		"col_numeric=$5 AND col_timestamptz=$6 AND col_date=$7 AND col_varchar=$8 AND col_jsonb::text=$9 AND col_bigint=$10"
	sd, err := conn.Prepare(ctx, "select_all_types", sql)
	if err != nil {
		return C.CString(err.Error())
	}
	wantParamTypes := []int{
		pgtype.Int8OID,
		pgtype.BoolOID,
		pgtype.ByteaOID,
		pgtype.Float8OID,
		pgtype.NumericOID,
		pgtype.TimestamptzOID,
		pgtype.DateOID,
		pgtype.VarcharOID,
		pgtype.VarcharOID,
		pgtype.Int8OID,
	}
	if g, w := len(sd.ParamOIDs), len(wantParamTypes); g != w {
		return C.CString(fmt.Sprintf("param type count mismatch:\n Got: %v\nWant: %v", g, w))
	}
	for i, tp := range wantParamTypes {
		if g, w := sd.ParamOIDs[i], uint32(tp); g != w {
			return C.CString(fmt.Sprintf("param type mismatch for param[%v]:\n Got: %v\nWant: %v", i, g, w))
		}
	}

	wantFieldTypes := []int{
		pgtype.Int8OID,
		pgtype.BoolOID,
		pgtype.ByteaOID,
		pgtype.Float8OID,
		pgtype.Int8OID,
		pgtype.NumericOID,
		pgtype.TimestamptzOID,
		pgtype.DateOID,
		pgtype.JSONBOID,
		pgtype.VarcharOID,
	}
	if g, w := len(sd.Fields), len(wantFieldTypes); g != w {
		return C.CString(fmt.Sprintf("field count mismatch:\n Got: %v\nWant: %v", g, w))
	}
	for i, tp := range wantParamTypes {
		if g, w := sd.ParamOIDs[i], uint32(tp); g != w {
			return C.CString(fmt.Sprintf("param type mismatch for param[%v]:\n Got: %v\nWant: %v", i, g, w))
		}
	}

	return nil
}

//export TestInsertBatch
func TestInsertBatch(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	batch := &pgx.Batch{}
	batchSize := 10
	if err := insertBatch(batch, connString, batchSize); err != nil {
		return C.CString(err.Error())
	}
	res := conn.SendBatch(ctx, batch)
	for i := 0; i < batchSize; i++ {
		tag, err := res.Exec()
		if err != nil {
			return C.CString(fmt.Sprintf("failed to execute insert statement %d: %v", i, err))
		}
		if !tag.Insert() {
			return C.CString(fmt.Sprintf("statement %d was not recognized as an insert", i))
		}
		if g, w := tag.RowsAffected(), int64(1); g != w {
			return C.CString(fmt.Sprintf("rows affected mismatch for statement %d:\n Got: %v\nWant: %v", i, g, w))
		}
	}
	if err := res.Close(); err != nil {
		return C.CString(err.Error())
	}

	return nil
}

//export TestMixedBatch
func TestMixedBatch(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	batch := &pgx.Batch{}
	batchSize := 5
	if err := insertBatch(batch, connString, batchSize); err != nil {
		return C.CString(err.Error())
	}
	batch.Queue("select count(*) from all_types where col_bool=$1", true)
	batch.Queue("update all_types set col_bool=false where col_bool=$1", true)

	res := conn.SendBatch(ctx, batch)
	for i := 0; i < batchSize; i++ {
		tag, err := res.Exec()
		if err != nil {
			return C.CString(fmt.Sprintf("failed to execute insert statement %d: %v", i, err))
		}
		if !tag.Insert() {
			return C.CString(fmt.Sprintf("statement %d was not recognized as an insert", i))
		}
		if g, w := tag.RowsAffected(), int64(1); g != w {
			return C.CString(fmt.Sprintf("rows affected mismatch for statement %d:\n Got: %v\nWant: %v", i, g, w))
		}
	}
	var count int64
	if err := res.QueryRow().Scan(&count); err != nil {
		return C.CString(fmt.Sprintf("failed to get row count: %v", err.Error()))
	}
	tag, err := res.Exec()
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute update: %v", err.Error()))
	}
	if !tag.Update() {
		return C.CString("update statement was not recognized as an update")
	}
	if g, w := tag.RowsAffected(), count; g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch for update statement:\n Got: %v\nWant: %v", g, w))
	}
	if err := res.Close(); err != nil {
		return C.CString(err.Error())
	}

	return nil
}

//export TestBatchError
func TestBatchError(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	batch := &pgx.Batch{}
	batchSize := 5
	if err := insertBatch(batch, connString, batchSize); err != nil {
		return C.CString(err.Error())
	}
	// This statement will fail.
	batch.Queue("select count(*) from non_existent_table where col_bool=$1", true)
	// This statement will not be executed as the previous statement failed.
	batch.Queue("update all_types set col_bool=false where col_bool=$1", true)

	res := conn.SendBatch(ctx, batch)

	// Try to get results from the batch execution. Even though the error occurred for the select
	// statement, it is returned for the first statement in the batch.
	_, err = res.Exec()
	if err == nil {
		return C.CString(fmt.Sprintf("expected error for batch, but got nil"))
	}
	if err := res.Close(); err != nil {
		return C.CString(err.Error())
	}

	return nil
}

//export TestBatchExecutionError
func TestBatchExecutionError(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	batch := &pgx.Batch{}
	batchSize := 3
	if err := insertBatch(batch, connString, batchSize); err != nil {
		return C.CString(err.Error())
	}

	res := conn.SendBatch(ctx, batch)

	// Try to get results from the batch execution.
	tag, err := res.Exec()
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute first insert statement: %v", err))
	}
	if !tag.Insert() {
		return C.CString("the first statement was not recognized as an insert")
	}
	if g, w := tag.RowsAffected(), int64(1); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch for first statement:\n Got: %v\nWant: %v", g, w))
	}

	_, err = res.Exec()
	if err == nil {
		return C.CString(fmt.Sprintf("expected error for second statement, but got nil"))
	}
	if err := res.Close(); err != nil {
		return C.CString(fmt.Sprintf("closing batch result returned error: %v", err.Error()))
	}

	return nil
}

func insertBatch(batch *pgx.Batch, connString string, batchSize int) error {
	sql := "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	numeric := pgtype.Numeric{}
	for i := 0; i < batchSize; i++ {
		_ = numeric.Set(strconv.Itoa(i) + ".123")
		var timestamptz interface{}
		var date interface{}
		// TODO: Remove this when the backend supports Zulu timestamp/date literals.
		if strings.Contains(connString, "prefer_simple_protocol=true") {
			date = fmt.Sprintf("2022-04-%02d", i+1)
			timestamptz = fmt.Sprintf("2022-03-24 %02d:39:10.123456000+00", i)
		} else {
			date = &pgtype.Date{}
			_ = date.(*pgtype.Date).Set(fmt.Sprintf("2022-04-%02d", i+1))
			timestamptz, _ = time.Parse(time.RFC3339Nano, fmt.Sprintf("2022-03-24T%02d:39:10.123456000Z", i))
		}
		batch.Queue(sql, 100+i, i%2 == 0, []byte(strconv.Itoa(i)+"test_bytes"), 3.14+float64(i), i, numeric, timestamptz, date, "test_string"+strconv.Itoa(i), fmt.Sprintf("{\"key\": \"value%v\"}", i))
	}
	return nil
}

//export TestWrongDialect
func TestWrongDialect(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(fmt.Sprintf("failed to connect to PG: %v", err))
	}
	defer conn.Close(ctx)

	return nil
}

//export TestCopyIn
func TestCopyIn(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	numeric := pgtype.Numeric{}
	numeric.Set("6.626")
	timestamptz, _ := time.Parse(time.RFC3339Nano, "2022-03-24T12:39:10.123456000Z")
	date := pgtype.Date{}
	date.Set("2022-07-01")
	jsonb := pgtype.JSONB{}
	jsonb.Set("{\"key\": \"value\"}")
	rows := [][]interface{}{
		{1, true, []byte{1, 2, 3}, float32(3.14), 3.14, 10, numeric, timestamptz, date, "test", jsonb},
		{2, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
	}
	count, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{"all_types"},
		[]string{"col_bigint", "col_bool", "col_bytea", "col_float4", "col_float8", "col_int", "col_numeric", "col_timestamptz", "col_date", "col_varchar", "col_jsonb"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return C.CString(fmt.Sprintf("failed to execute COPY statement: %v", err))
	}
	if g, w := count, int64(2); g != w {
		return C.CString(fmt.Sprintf("rows affected mismatch:\n Got: %v\nWant: %v", g, w))
	}

	return nil
}

//export TestReadWriteTransaction
func TestReadWriteTransaction(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return C.CString(fmt.Sprintf("failed to begin transaction: %v", err.Error()))
	}

	// Execute a query in a read/write transaction.
	var value int64
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&value)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := value, int64(1); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	sql := "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	numeric := pgtype.Numeric{}
	_ = numeric.Set("6.626")
	timestamptz, _ := time.Parse(time.RFC3339Nano, "2022-03-24T07:39:10.123456789+01:00")
	var tag pgconn.CommandTag
	date := pgtype.Date{}
	_ = date.Set("2022-04-02")
	for _, id := range []int64{10, 20} {
		if strings.Contains(connString, "prefer_simple_protocol=true") {
			// Simple mode will format the date as '2022-04-02 00:00:00Z', which is not supported by the
			// backend yet.
			tag, err = tx.Exec(ctx, sql, id, true, []byte("test_bytes"), 3.14, 1, numeric, timestamptz, "2022-04-02", "test_string", "{\"key\": \"value\"}")
		} else {
			tag, err = tx.Exec(ctx, sql, id, true, []byte("test_bytes"), 3.14, 1, numeric, timestamptz, date, "test_string", "{\"key\": \"value\"}")
		}
		if err != nil {
			return C.CString(fmt.Sprintf("failed to execute insert statement: %v", err))
		}
		if !tag.Insert() {
			return C.CString("statement was not recognized as an insert")
		}
		if g, w := tag.RowsAffected(), int64(1); g != w {
			return C.CString(fmt.Sprintf("rows affected mismatch:\n Got: %v\nWant: %v", g, w))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return C.CString(fmt.Sprintf("failed to commit transaction: %v", err))
	}

	return nil
}

//export TestReadOnlyTransaction
func TestReadOnlyTransaction(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return C.CString(fmt.Sprintf("failed to begin read-only transaction: %v", err.Error()))
	}
	for _, i := range []int{1, 2} {
		var value int64
		err = tx.QueryRow(ctx, fmt.Sprintf("SELECT %d", i)).Scan(&value)
		if err != nil {
			return C.CString(err.Error())
		}
		if g, w := value, int64(i); g != w {
			return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return C.CString(fmt.Sprintf("failed to commit read-only transaction: %v", err.Error()))
	}

	return nil
}

//export TestReadWriteTransactionIsolationLevelSerializable
func TestReadWriteTransactionIsolationLevelSerializable(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return C.CString(fmt.Sprintf("failed to begin transaction: %v", err.Error()))
	}

	var value int64
	err = tx.QueryRow(ctx, "SELECT 1").Scan(&value)
	if err != nil {
		return C.CString(err.Error())
	}
	if g, w := value, int64(1); g != w {
		return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
	}

	if err := tx.Commit(ctx); err != nil {
		return C.CString(fmt.Sprintf("failed to commit transaction: %v", err))
	}

	return nil
}

//export TestReadWriteTransactionIsolationLevelRepeatableRead
func TestReadWriteTransactionIsolationLevelRepeatableRead(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	_, err = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err == nil {
		return C.CString("missing expected error for BeginTx with isolation level RepeatableRead")
	}
	if g, w := err.Error(), "ERROR: Unknown statement: begin isolation level repeatable read (SQLSTATE P0001)"; g != w {
		return C.CString(fmt.Sprintf("error mismatch\nGot:  %v\nWant: %v", g, w))
	}

	return nil
}

//export TestReadOnlySerializableTransaction
func TestReadOnlySerializableTransaction(connString string) *C.char {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return C.CString(err.Error())
	}
	defer conn.Close(ctx)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.Serializable})
	if err != nil {
		return C.CString(fmt.Sprintf("failed to begin read-only transaction with isolation level serializable: %v", err.Error()))
	}
	for _, i := range []int{1, 2} {
		var value int64
		err = tx.QueryRow(ctx, fmt.Sprintf("SELECT %d", i)).Scan(&value)
		if err != nil {
			return C.CString(err.Error())
		}
		if g, w := value, int64(i); g != w {
			return C.CString(fmt.Sprintf("value mismatch\n Got: %v\nWant: %v", g, w))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return C.CString(fmt.Sprintf("failed to commit read-only transaction: %v", err.Error()))
	}

	return nil
}
