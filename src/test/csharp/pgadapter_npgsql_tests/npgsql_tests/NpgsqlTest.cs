using System.Data;
using System.Globalization;
using Npgsql;
using System.Reflection;
using System.Text;

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
                var stringValue = System.Text.Encoding.UTF8.GetString(buffer);
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
            }
        }
        Console.WriteLine("Success");
    }

    public void TestInsertAllDataTypes()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();

        var sql =
            "INSERT INTO all_types (col_bigint, col_bool, col_bytea, col_float8, col_numeric, col_timestamptz, col_date, col_varchar)"
            + " values ($1, $2, $3, $4, $5, $6, $7, $8)";
        using var cmd = new NpgsqlCommand(sql, connection)
        {
            Parameters =
            {
                new () {Value = 100L},
                new () {Value = true},
                new () {Value = Encoding.UTF8.GetBytes("test_bytes")},
                new () {Value = 3.14d},
                new () {Value = 6.626m},
                new () {Value = DateTime.Parse("2022-03-24T08:39:10.1234568+02:00").ToUniversalTime(), DbType = DbType.DateTimeOffset},
                new () {Value = DateTime.Parse("2022-04-02"), DbType = DbType.Date},
                new () {Value = "test_string"},
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
            + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar) "
            + "values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
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
    
}
