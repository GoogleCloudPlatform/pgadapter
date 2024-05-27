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

// [START spanner_update_data]
using Npgsql;

namespace dotnet_snippets;

static class UpdateDataWithCopySample
{
    internal static void UpdateDataWithCopy(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
        // will succeed even if it exceeds Spanner's mutation limit per transaction.
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "set spanner.autocommit_dml_mode='partitioned_non_atomic'";
        cmd.ExecuteNonQuery();
        
        // Instruct PGAdapter to use insert-or-update for COPY statements.
        // This enables us to use COPY to update existing data.
        cmd.CommandText = "set spanner.copy_upsert=true";
        cmd.ExecuteNonQuery();
        
        // COPY uses mutations to insert or update existing data in Spanner.
        using (var albumWriter = connection.BeginTextImport(
                   "COPY albums (singer_id, album_id, marketing_budget) FROM STDIN"))
        {
            albumWriter.WriteLine("1\t1\t100000");
            albumWriter.WriteLine("2\t2\t500000");
        }
        Console.WriteLine($"Updated 2 albums");
    }
}
// [END spanner_update_data]
