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

// [START spanner_statement_timeout]
using Npgsql;

namespace dotnet_snippets;

static class QueryWithTimeoutSample
{
    internal static void QueryWithTimeout(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT singer_id, album_id, album_title "
                                          + "FROM albums "
                                          + "WHERE album_title in ("
                                          + "  SELECT first_name "
                                          + "  FROM singers "
                                          + "  WHERE last_name LIKE '%a%'"
                                          + "     OR last_name LIKE '%m%'"
                                          + ")", connection);
        // npgsql has built-in support for setting query timeouts.
        // This sets the query timeout for this statement to 5 seconds.
        cmd.CommandTimeout = 5;
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine($"{reader["singer_id"]} {reader["album_id"]} {reader["album_title"]}");
        }
    }
}
// [END spanner_statement_timeout]
