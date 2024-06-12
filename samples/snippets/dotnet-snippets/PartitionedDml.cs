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

// [START spanner_partitioned_dml]
using Npgsql;

namespace dotnet_snippets;

public static class PartitionedDmlSample
{
    public static void PartitionedDml(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Enable Partitioned DML on this connection.
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "set spanner.autocommit_dml_mode='partitioned_non_atomic'";
        cmd.ExecuteNonQuery();

        // Back-fill a default value for the MarketingBudget column.
        cmd.CommandText = "update albums set marketing_budget=0 where marketing_budget is null";
        var lowerBoundUpdateCount = cmd.ExecuteNonQuery();
        
        Console.WriteLine($"Updated at least {lowerBoundUpdateCount} albums");
    }
}
// [END spanner_partitioned_dml]
