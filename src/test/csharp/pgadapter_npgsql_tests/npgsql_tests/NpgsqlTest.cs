﻿// Copyright 2022 Google LLC
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

using System.Data;
using Npgsql;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace npgsql_tests;

public class NpgsqlTest
{
    public static void Main(string[] args)
    {
        if (args.Length != 2)
        {
            throw new ArgumentException($"Unexpected number of arguments: " +
                                        $"Got {args.Length}, Want 2: TestName ConnectionString");
        }
        var test = new NpgsqlTest(args[0], args[1]);
        test.Execute();
    }

    private string Test { get; }
    
    private string ConnectionString { get; }

    private NpgsqlTest(string test, string connectionString)
    {
        Test = test;
        ConnectionString = connectionString;
    }

    private void Execute()
    {
        MethodInfo method = typeof(NpgsqlTest).GetMethod(Test, BindingFlags.Public | BindingFlags.Instance)!;
        try
        {
            method.Invoke(this, null);
        }
        catch (TargetInvocationException exception)
        {
            Console.WriteLine(exception.InnerException!.Message);
            Console.WriteLine(exception.InnerException!.StackTrace);
            throw;
        }
    }

    public void TestShowServerVersion()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("show server_version", connection);
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var version = reader.GetString(0);
                Console.WriteLine($"{version}");
            }
        }
    }

    public void TestSelect1()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT 1", connection);
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var got = reader.GetInt64(0);
                if (got == 1L)
                {
                    continue;
                }
                
                Console.WriteLine($"Value mismatch: Got {got}, Want: 1");
                return;
            }
        }
        Console.WriteLine("Success");
    }

    public void TestQueryWithParameter()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM FOO WHERE BAR=$1", connection)
        {
            Parameters =
            {
                new() {Value = "baz"}
            }
        };
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var got = reader.GetString(0);
                if (got == "baz")
                {
                    continue;
                }
                
                Console.WriteLine($"Value mismatch: Got '{got}', Want: 'baz'");
                return;
            }
        }
        Console.WriteLine("Success");
    }

    public void TestQueryAllDataTypes()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT * FROM all_types WHERE col_bigint=1", connection);
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                int index = -1;
                if (reader.GetInt64(++index) != 1L)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetInt64(index)}', Want: 1");
                    return;
                }
                if (!reader.GetBoolean(++index))
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetBoolean(index)}', Want: true");
                    return;
                }
                
                var length = (int) reader.GetBytes(++index, 0, null, 0, Int32.MaxValue);
                byte[] buffer = new byte[length];
                reader.GetBytes(index, 0, buffer, 0, length);
                var stringValue = Encoding.UTF8.GetString(buffer);
                if (stringValue != "test")
                {
                    Console.WriteLine($"Value mismatch: Got '{stringValue}', Want: 'test'");
                    return;
                }
                if (Math.Abs(reader.GetDouble(++index) - 3.14d) > 0.0d)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetDouble(index)}', Want: 3.14");
                    return;
                }
                if (reader.GetInt32(++index) != 100)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetInt32(index)}', Want: 100");
                    return;
                }
                if (reader.GetDecimal(++index) != 6.626m)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetDecimal(index)}', Want: 6.626");
                    return;
                }
                // Note: The timestamp value is truncated to millisecond precision.
                var wantTimestamp = DateTime.Parse("2022-02-16T13:18:02.123456");
                var gotTimestamp = reader.GetDateTime(++index);
                if (gotTimestamp != wantTimestamp)
                {
                    var format = "yyyyMMddTHHmmssFFFFFFF";
                    Console.WriteLine($"Value mismatch: Got '{gotTimestamp.ToString(format)}', Want: '{wantTimestamp.ToString(format)}'");
                    return;
                }
                if (reader.GetDateTime(++index) != DateTime.Parse("2022-03-29"))
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetDateTime(index)}', Want: '2022-03-29'");
                    return;
                }
                if (reader.GetString(++index) != "test")
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetString(index)}', Want: 'test'");
                    return;
                }
                if (reader.GetString(++index) != "{\"key\": \"value\"}")
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetString(index)}', Want: '{{\"key\": \"value\"}}'");
                    return;
                }
            }
        }
        Console.WriteLine("Success");
    }

    public void TestUpdateAllDataTypes()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "UPDATE all_types SET col_bigint=$1, col_bool=$2, col_bytea=$3, col_float8=$4, col_int=$5, col_numeric=$6, " +
            "col_timestamptz=$7, col_date=$8, col_varchar=$9, col_jsonb=$10 WHERE col_varchar = $11";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
                new () {Value = 100L},
                new () {Value = true},
                new () {Value = Encoding.UTF8.GetBytes("test_bytes")},
                new () {Value = 3.14d},
                new () {Value = 1},
                new () {Value = 6.626m},
                new () {Value = DateTime.Parse("2022-03-24T06:39:10.123456000Z").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                new () {Value = DateTime.Parse("2022-04-02"), DbType = DbType.Date},
                new () {Value = "test_string"},
                new () {Value = JsonDocument.Parse("{\"key\": \"value\"}")},
                new () {Value = "test"},
            }
        };
        var updateCount = cmd.ExecuteNonQuery();
        if (updateCount != 1)
        {
            Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: 1");
            return;
        }
        Console.WriteLine("Success");
    }

    public void TestInsertAllDataTypes()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
                new () {Value = 100L},
                new () {Value = true},
                new () {Value = Encoding.UTF8.GetBytes("test_bytes")},
                new () {Value = 3.14d},
                new () {Value = 100},
                new () {Value = 6.626m},
                new () {Value = DateTime.Parse("2022-03-24T08:39:10.1234568+02:00").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                new () {Value = DateTime.Parse("2022-04-02"), DbType = DbType.Date},
                new () {Value = "test_string"},
                new () {Value = JsonDocument.Parse("{\"key\":\"value\"}")},
            }
        };
        var updateCount = cmd.ExecuteNonQuery();
        if (updateCount != 1)
        {
            Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: 1");
            return;
        }
        Console.WriteLine("Success");
    }

    public void TestInsertNullsAllDataTypes()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types "
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
                new () {Value = 100L},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
                new () {Value = DBNull.Value},
            }
        };
        var updateCount = cmd.ExecuteNonQuery();
        if (updateCount != 1)
        {
            Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: 1");
            return;
        }
        Console.WriteLine("Success");
    }

    public void TestInsertAllDataTypesReturning()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning *";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
                new () {Value = 1L},
                new () {Value = true},
                new () {Value = Encoding.UTF8.GetBytes("test_bytes")},
                new () {Value = 3.14d},
                new () {Value = 100},
                new () {Value = 6.626m},
                new () {Value = DateTime.Parse("2022-02-16T13:18:02.123456789Z").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                new () {Value = DateTime.Parse("2022-03-29"), DbType = DbType.Date},
                new () {Value = "test_string"},
                new () {Value = JsonDocument.Parse("{\"key\":\"value\"}")},
            }
        };
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                int index = -1;
                if (reader.GetInt64(++index) != 1L)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetInt64(index)}', Want: 1");
                    return;
                }
                if (!reader.GetBoolean(++index))
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetBoolean(index)}', Want: true");
                    return;
                }
                
                var length = (int) reader.GetBytes(++index, 0, null, 0, Int32.MaxValue);
                byte[] buffer = new byte[length];
                reader.GetBytes(index, 0, buffer, 0, length);
                var stringValue = Encoding.UTF8.GetString(buffer);
                if (stringValue != "test")
                {
                    Console.WriteLine($"Value mismatch: Got '{stringValue}', Want: 'test'");
                    return;
                }
                if (Math.Abs(reader.GetDouble(++index) - 3.14d) > 0.0d)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetDouble(index)}', Want: 3.14");
                    return;
                }
                if (reader.GetInt32(++index) != 100)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetInt32(index)}', Want: 100");
                    return;
                }
                if (reader.GetDecimal(++index) != 6.626m)
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetDecimal(index)}', Want: 6.626");
                    return;
                }
                var wantTimestamp = DateTime.Parse("2022-02-16T13:18:02.123456");
                var gotTimestamp = reader.GetDateTime(++index);
                if (gotTimestamp != wantTimestamp)
                {
                    var format = "yyyyMMddTHHmmssFFFFFFF";
                    Console.WriteLine($"Timestamp value mismatch: Got '{gotTimestamp.ToString(format)}', Want: '{wantTimestamp.ToString(format)}'");
                    return;
                }
                if (reader.GetDateTime(++index) != DateTime.Parse("2022-03-29"))
                {
                    Console.WriteLine($"Date value mismatch: Got '{reader.GetDateTime(index)}', Want: '2022-03-29'");
                    return;
                }
                if (reader.GetString(++index) != "test")
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetString(index)}', Want: 'test'");
                    return;
                }
                if (reader.GetString(++index) != "{\"key\": \"value\"}")
                {
                    Console.WriteLine($"Value mismatch: Got '{reader.GetString(index)}', Want: '{{\"key\": \"value\"}}'");
                    return;
                }
            }
        }
        Console.WriteLine("Success");
    }

    public void TestInsertBatch()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

        var batchSize = 10;
        using var batch = new NpgsqlBatch(connection);
        for (var i = 0; i < batchSize; i++)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand(sql)
            {
                Parameters =
                {
                    new () {Value = 100L + i},
                    new () {Value = i%2 == 0},
                    new () {Value = Encoding.UTF8.GetBytes(i + "test_bytes")},
                    new () {Value = 3.14d + i},
                    new () {Value = i},
                    new () {Value = i + 0.123m},
                    new () {Value = DateTime.Parse($"2022-03-24 {i:D2}:39:10.123456000+00").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                    new () {Value = DateTime.Parse($"2022-04-{i+1:D2}"), DbType = DbType.Date},
                    new () {Value = "test_string" + i},
                    new () {Value = JsonDocument.Parse($"{{\"key\":\"value{i}\"}}")},
                }
            });
        }
        var updateCount = batch.ExecuteNonQuery();
        if (updateCount != batchSize)
        {
            Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: {batchSize}");
            return;
        }
        Console.WriteLine("Success");
    }
    
}
