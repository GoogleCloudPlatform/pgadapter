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

using System.Data;
using Npgsql;
using NpgsqlTypes;
using System.Globalization;
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

    public void TestShowApplicationName()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("show application_name", connection);
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var applicationName = reader.GetString(0);
                Console.WriteLine($"{applicationName}");
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

    public void TestSelectArray()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT '{1,2}'::bigint[] as c", connection);
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                var got = reader.GetFieldValue<long?[]>(0);
                if (got.Length == 2 && got[0] == 1L && got[1] == 2L)
                {
                    continue;
                }
                
                Console.WriteLine($"Value mismatch: Got {got}, Want: (1,2)");
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
            "UPDATE all_types SET col_bool=$1, col_bytea=$2, col_float8=$3, col_int=$4, col_numeric=$5, " +
            "col_timestamptz=$6, col_date=$7, col_varchar=$8, col_jsonb=$9 WHERE col_varchar = $10";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
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
                new () {Value = Encoding.UTF8.GetBytes("test")},
                new () {Value = 3.14d},
                new () {Value = 100},
                new () {Value = 6.626m},
                new () {Value = DateTime.Parse("2022-02-16T13:18:02.123456789Z").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                new () {Value = DateTime.Parse("2022-03-29"), DbType = DbType.Date},
                new () {Value = "test"},
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

    public void TestMixedBatch()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

        var batchSize = 5;
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
        batch.BatchCommands.Add(new NpgsqlBatchCommand("select count(*) from all_types where col_bool=$1")
        {
            Parameters = { new () {Value = true} }
        });
        batch.BatchCommands.Add(new NpgsqlBatchCommand("update all_types set col_bool=false where col_bool=$1")
        {
            Parameters = { new () {Value = true} }
        });
        using (var reader = batch.ExecuteReader())
        {
            if (reader.RecordsAffected != batchSize)
            {
                Console.WriteLine($"Insert count mismatch. Got: {reader.Rows}, Want: {batchSize}");
                return;
            }
            if (!reader.Read())
            {
                Console.WriteLine("Missing expected row");
                return;
            }
            if (reader.GetInt64(0) != 3L)
            {
                Console.WriteLine($"Value mismatch: Got '{reader.GetInt64(0)}', Want: 3");
                return;
            }
            if (reader.Read())
            {
                Console.WriteLine("Got unexpected expected row");
                return;
            }
            if (reader.NextResult())
            {
                Console.WriteLine("Unexpected result set after select");
                return;
            }
            // npgsql returns the total number of rows affected for the entire batch until here.
            if (reader.RecordsAffected != 8)
            {
                Console.WriteLine($"Update count mismatch. Got: {reader.RecordsAffected}, Want: 8");
                return;
            }
            if (reader.NextResult())
            {
                Console.WriteLine("Unexpected result after update");
                return;
            }
            if (reader.RecordsAffected != 8)
            {
                Console.WriteLine($"Unexpected update count after last result. Got {reader.RecordsAffected}, Want: 8");
                return;
            }
        }
        Console.WriteLine("Success");
    }

    public void TestBatchExecutionError()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

        var batchSize = 3;
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
        try
        {
            var updateCount = batch.ExecuteNonQuery();
            Console.WriteLine($"Update count: {updateCount}");
        }
        catch (Exception exception)
        {
            Console.WriteLine($"Executing batch failed with error: {exception.Message}");
        }
    }

    public void TestDdlBatch()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var command = new NpgsqlBatch(connection);
        command.BatchCommands.Add(new NpgsqlBatchCommand(@"
            create table singers (
                id         varchar not null primary key,
                first_name varchar,
                last_name  varchar not null,
                full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
                active     boolean,
                created_at timestamptz,
                updated_at timestamptz
            )"));
        command.BatchCommands.Add(new NpgsqlBatchCommand(@"
            create table albums (
                id               varchar not null primary key,
                title            varchar not null,
                marketing_budget numeric,
                release_date     date,
                cover_picture    bytea,
                singer_id        varchar not null,
                created_at       timestamptz,
                updated_at       timestamptz,
                constraint fk_albums_singers foreign key (singer_id) references singers (id)
            )"));
        command.BatchCommands.Add(new NpgsqlBatchCommand(@"
            create table tracks (
                id           varchar not null,
                track_number bigint not null,
                title        varchar not null,
                sample_rate  float8 not null,
                created_at   timestamptz,
                updated_at   timestamptz,
                primary key (id, track_number)
            ) interleave in parent albums on delete cascade"));
        command.BatchCommands.Add(new NpgsqlBatchCommand(@"
            create table venues (
                id          varchar not null primary key,
                name        varchar not null,
                description varchar not null,
                created_at  timestamptz,
                updated_at  timestamptz
            )"));
        command.BatchCommands.Add(new NpgsqlBatchCommand(@"
            create table concerts (
            id          varchar not null primary key,
            venue_id    varchar not null,
            singer_id   varchar not null,
            name        varchar not null,
            start_time  timestamptz not null,
            end_time    timestamptz not null,
            created_at  timestamptz,
            updated_at  timestamptz,
            constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
            constraint fk_concerts_singers foreign key (singer_id) references singers (id),
            constraint chk_end_time_after_start_time check (end_time > start_time)
        )"));
        var updateCount = command.ExecuteNonQuery();
        if (updateCount != -1)
        {
            Console.WriteLine($"Batch returned {updateCount} updates, expected -1.");
            return;
        }
        Console.WriteLine("Success");
    }

    public void TestDdlScript()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        using var command = new NpgsqlCommand(@"
            -- Executing the schema creation in a batch will improve execution speed.
            start batch ddl;

            create table singers (
                id         varchar not null primary key,
                first_name varchar,
                last_name  varchar not null,
                full_name  varchar generated always as (coalesce(concat(first_name, ' '::varchar, last_name), last_name)) stored,
                active     boolean,
                created_at timestamptz,
                updated_at timestamptz
            );

            create table albums (
                id               varchar not null primary key,
                title            varchar not null,
                marketing_budget numeric,
                release_date     date,
                cover_picture    bytea,
                singer_id        varchar not null,
                created_at       timestamptz,
                updated_at       timestamptz,
                constraint fk_albums_singers foreign key (singer_id) references singers (id)
            );

            create table tracks (
                id           varchar not null,
                track_number bigint not null,
                title        varchar not null,
                sample_rate  float8 not null,
                created_at   timestamptz,
                updated_at   timestamptz,
                primary key (id, track_number)
            ) interleave in parent albums on delete cascade;

            create table venues (
                id          varchar not null primary key,
                name        varchar not null,
                description varchar not null,
                created_at  timestamptz,
                updated_at  timestamptz
            );

            create table concerts (
                id          varchar not null primary key,
                venue_id    varchar not null,
                singer_id   varchar not null,
                name        varchar not null,
                start_time  timestamptz not null,
                end_time    timestamptz not null,
                created_at  timestamptz,
                updated_at  timestamptz,
                constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),
                constraint fk_concerts_singers foreign key (singer_id) references singers (id),
                constraint chk_end_time_after_start_time check (end_time > start_time)
            );

            run batch;", connection);
        
        var updateCount = command.ExecuteNonQuery();
        if (updateCount != -1)
        {
            Console.WriteLine($"Batch returned {updateCount} updates, expected -1.");
            return;
        }
        Console.WriteLine("Success");
    }

    public void TestBinaryCopyIn()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        
        using (var writer =
               connection.BeginBinaryImport("COPY all_types " +
                                            "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " +
                                            "FROM STDIN (FORMAT BINARY)"))
        {
            writer.StartRow();
            writer.Write(1L);
            writer.Write(true);
            writer.Write(new byte[] {1,2,3});
            writer.Write(3.14d);
            writer.Write(10);
            writer.Write(6.626m);
            writer.Write(DateTime.Parse("2022-03-24 12:39:10.123456000Z").ToUniversalTime());
            writer.Write(DateTime.Parse("2022-07-01"), NpgsqlDbType.Date);
            writer.Write("test");
            writer.Write(JsonDocument.Parse("{\"key\": \"value\"}"));

            writer.StartRow();
            writer.Write(2L);
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();
            writer.WriteNull();

            writer.Complete();
        }
        
        Console.WriteLine("Success");
    }

    public void TestTextCopyIn()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        
        using (var writer =
               connection.BeginTextImport("COPY all_types " +
                                            "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " +
                                            "FROM STDIN"))
        {
            writer.Write("1\t");
            writer.Write("t\t");
            writer.Write("\\x010203\t");
            writer.Write("3.14\t");
            writer.Write("10\t");
            writer.Write("6.626\t");
            writer.Write("2022-03-24 12:39:10.123456Z\t");
            writer.Write("2022-07-01\t");
            writer.Write("test\t");
            writer.Write("{\"key\":\"value\"}");
            writer.Write("\n");

            writer.Write("2\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\t");
            writer.Write("\\N\n");
        }
        
        Console.WriteLine("Success");
    }

    public void TestBinaryCopyOut()
    {
        var nfi = new NumberFormatInfo
        {
            NumberDecimalSeparator = "."
        };
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        
        using (var reader =
               connection.BeginBinaryExport("COPY all_types " +
                                            "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " +
                                            "TO STDOUT (FORMAT BINARY)"))
        {
            while (reader.StartRow() > -1)
            {
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<long>(NpgsqlDbType.Bigint));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<bool>(NpgsqlDbType.Boolean));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(Convert.ToBase64String(reader.Read<byte[]>(NpgsqlDbType.Bytea)));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<double>(NpgsqlDbType.Double).ToString(nfi));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<long>(NpgsqlDbType.Bigint));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<decimal>(NpgsqlDbType.Numeric).ToString(nfi));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<DateTime>(NpgsqlDbType.TimestampTz).ToUniversalTime().ToString("yyyyMMddTHHmmssFFFFFFF"));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<DateTime>(NpgsqlDbType.Date).ToString("yyyyMMdd"));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<string>(NpgsqlDbType.Varchar));
                }
                Console.Write("\t");
                if (reader.IsNull)
                {
                    Console.Write("NULL");
                    reader.Skip();
                }
                else
                {
                    Console.Write(reader.Read<string>(NpgsqlDbType.Jsonb));
                }
                Console.Write("\n");
            }
        }
        Console.WriteLine("Success");
    }

    public void TestTextCopyOut()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        
        using (var reader =
               connection.BeginTextExport("COPY all_types " +
                                            "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) " +
                                            "TO STDOUT"))
        {
            while (true)
            {
                var line = reader.ReadLine();
                if (line == null)
                {
                    break;
                }
                Console.WriteLine(line);
            }
        }
        Console.WriteLine("Success");
    }

    public void TestSimplePrepare()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        
        var cmd = new NpgsqlCommand("SELECT * FROM all_types WHERE col_bigint=$1", connection);
        cmd.Parameters.Add("param", NpgsqlDbType.Integer);
        cmd.Prepare();
        Console.WriteLine("Success");
    }

    public void TestPrepareAndExecute()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        // Prepare-and-execute twice to verify that the statement is only
        // parsed and prepared once, and executed twice.
        for (var i = 0; i < 2; i++)
        {
            var sql =
                "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
                + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
            var cmd = new NpgsqlCommand(sql, connection);
#pragma warning disable CS8625
            cmd.Parameters.Add(null, NpgsqlDbType.Bigint);
            cmd.Parameters.Add(null, NpgsqlDbType.Boolean);
            cmd.Parameters.Add(null, NpgsqlDbType.Bytea);
            cmd.Parameters.Add(null, NpgsqlDbType.Double);
            cmd.Parameters.Add(null, NpgsqlDbType.Integer);
            cmd.Parameters.Add(null, NpgsqlDbType.Numeric);
            cmd.Parameters.Add(null, NpgsqlDbType.TimestampTz);
            cmd.Parameters.Add(null, NpgsqlDbType.Date);
            cmd.Parameters.Add(null, NpgsqlDbType.Varchar);
            cmd.Parameters.Add(null, NpgsqlDbType.Jsonb);
#pragma warning restore CS8625
            cmd.Prepare();

            var index = 0;
            cmd.Parameters[index++].Value = 100L + i;
            cmd.Parameters[index++].Value = true;
            cmd.Parameters[index++].Value = Encoding.UTF8.GetBytes("test_bytes");
            cmd.Parameters[index++].Value = 3.14d;
            cmd.Parameters[index++].Value = 100;
            cmd.Parameters[index++].Value = 6.626m;
            cmd.Parameters[index++].Value = DateTime.Parse("2022-03-24T08:39:10.1234568+02:00").ToUniversalTime();
            cmd.Parameters[index++].Value = DateTime.Parse("2022-04-02");
            cmd.Parameters[index++].Value = "test_string";
            cmd.Parameters[index++].Value = JsonDocument.Parse("{\"key\":\"value\"}");
            var updateCount = cmd.ExecuteNonQuery();
            if (updateCount != 1)
            {
                Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: 1");
                return;
            }
        }
        Console.WriteLine("Success");
    }

    public void TestReadWriteTransaction()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        // The serialization level *MUST* be specified in npgsql. Otherwise,
        // npgsql will default to read-committed.
        var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        var selectCommand = new NpgsqlCommand("SELECT 1", connection);
        selectCommand.Transaction = transaction;
        using (var reader = selectCommand.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine("Row: " + reader.GetInt64(0));
            }
        }
        var insertSql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        foreach (var id in new [] {10, 20})
        {
            using var insertCommand = new NpgsqlCommand(insertSql, connection)
            {
                Parameters =
                {
                    new() { Value = id },
                    new() { Value = true },
                    new() { Value = Encoding.UTF8.GetBytes("test_bytes") },
                    new() { Value = 3.14d },
                    new() { Value = 100 },
                    new() { Value = 6.626m },
                    new()
                    {
                        Value = DateTime.Parse("2022-03-24T08:39:10.1234568+02:00").ToUniversalTime(),
                        DbType = DbType.DateTimeOffset
                    },
                    new() { Value = DateTime.Parse("2022-04-02"), DbType = DbType.Date },
                    new() { Value = "test_string" },
                    new() { Value = JsonDocument.Parse("{\"key\":\"value\"}") },
                }
            };
            insertCommand.Transaction = transaction;
            var updateCount = insertCommand.ExecuteNonQuery();
            if (updateCount != 1)
            {
                Console.WriteLine($"Update count mismatch. Got: {updateCount}, Want: 1");
                return;
            }
        }
        transaction.Commit();
        Console.WriteLine("Success");
    }

    public void TestReadOnlyTransaction()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        // The serialization level *MUST* be specified in npgsql. Otherwise,
        // npgsql will default to read-committed.
        var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        // npgsql and ADO.NET do not expose any native API creating a read-only transaction.
        new NpgsqlCommand("set transaction read only", connection, transaction).ExecuteNonQuery();

        foreach (var i in new[] { 1, 2 })
        {
            var selectCommand = new NpgsqlCommand($"SELECT {i}", connection, transaction);
            using var reader = selectCommand.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine("Row: " + reader.GetInt64(0));
            }
        }
        // We need to either commit or rollback the transaction to release the underlying resources.
        transaction.Commit();
        Console.WriteLine("Success");
    }
    
}
