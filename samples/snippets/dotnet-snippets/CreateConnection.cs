// Copyright 2024 Google LLC
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

// [START spanner_create_connection]
using Npgsql;

namespace dotnet_snippets;

public static class CreateConnectionSample
{
    public static void CreateConnection(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("select 'Hello World!' as hello", connection);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var greeting = reader.GetString(0);
            Console.WriteLine($"Greeting from Cloud Spanner PostgreSQL: {greeting}");
        }
    }
}
// [END spanner_create_connection]
