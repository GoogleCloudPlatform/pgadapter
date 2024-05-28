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

// [START spanner_copy_from_stdin]
using Npgsql;

namespace dotnet_snippets;

public static class WriteDataWithCopySample
{
    public static void WriteDataWithCopy(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
        // will succeed even if it exceeds Spanner's mutation limit per transaction.
        using var cmd = new NpgsqlCommand("set spanner.autocommit_dml_mode='partitioned_non_atomic'", connection);
        cmd.ExecuteNonQuery();
        
        var singerCount = 0;
        using var singerReader = new StreamReader("singers_data.txt");
        using (var singerWriter = connection.BeginTextImport("COPY singers (singer_id, first_name, last_name) FROM STDIN"))
        {
            while (singerReader.ReadLine() is { } line)
            {
                singerWriter.WriteLine(line);
                singerCount++;
            }
        }
        Console.WriteLine($"Copied {singerCount} singers");
        
        var albumCount = 0;
        using var albumReader = new StreamReader("albums_data.txt");
        using (var albumWriter = connection.BeginTextImport("COPY albums FROM STDIN"))
        {
            while (albumReader.ReadLine() is { } line)
            {
                albumWriter.WriteLine(line);
                albumCount++;
            }
        }
        Console.WriteLine($"Copied {albumCount} albums");
    }
}
// [END spanner_copy_from_stdin]
