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

// [START spanner_data_boost]
using Npgsql;

namespace dotnet_snippets;

public static class DataBoostSample
{
    public static void DataBoost(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        // This enables Data Boost for all partitioned queries on this connection.
        cmd.CommandText = "set spanner.data_boost_enabled=true";
        cmd.ExecuteNonQuery();


        // Run a partitioned query. This query will use Data Boost.
        cmd.CommandText = "run partitioned query "
                          + "select singer_id, first_name, last_name "
                          + "from singers";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine($"{reader["singer_id"]} {reader["first_name"]} {reader["last_name"]}");
        }
    }
}
// [END spanner_data_boost]
