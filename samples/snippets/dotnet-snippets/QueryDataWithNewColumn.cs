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

// [START spanner_query_data_with_new_column]
using Npgsql;

namespace dotnet_snippets;

public static class QueryDataWithNewColumnSample
{
    public static void QueryWithNewColumnData(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT singer_id, album_id, marketing_budget "
                                          + "FROM albums "
                                          + "ORDER BY singer_id, album_id", connection);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine($"{reader["singer_id"]} {reader["album_id"]} {reader["marketing_budget"]}");
        }
    }
}
// [END spanner_query_data_with_new_column]
