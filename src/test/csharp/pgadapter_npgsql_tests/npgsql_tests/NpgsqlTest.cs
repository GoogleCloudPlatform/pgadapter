using Npgsql;
using System.Reflection;

namespace npgsql_tests;

public class NpgsqlTest
{
    public static void Main(string[] args)
    {
        if (args.Length != 2)
        {
            throw new ArgumentException($"Unexpected number of arguments: Got {args.Length}, Want 2: TestName ConnectionString");
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
            Console.WriteLine(exception.InnerException!.StackTrace);
            throw;
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

    public void Ping()
    {
        Console.WriteLine("Here is C#");
    }
}
