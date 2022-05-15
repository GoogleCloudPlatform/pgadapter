namespace npgsql_tests;

public class NpgsqlTest
{
    public static void Main(string[] args)
    {
        new NpgsqlTest().Ping();
    }
    
    public void Ping()
    {
        Console.WriteLine("Here is C#");
    }
}