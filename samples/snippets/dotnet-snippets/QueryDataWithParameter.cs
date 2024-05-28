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

// [START spanner_query_with_parameter]
using Npgsql;

namespace dotnet_snippets;

public static class QueryDataWithParameterSample
{
    public static void QueryDataWithParameter(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("SELECT singer_id, first_name, last_name "
                                          + "FROM singers "
                                          + "WHERE last_name = $1", connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = "Garcia" });
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine($"{reader["singer_id"]} {reader["first_name"]} {reader["last_name"]}");
        }
    }
}
// [END spanner_query_with_parameter]
