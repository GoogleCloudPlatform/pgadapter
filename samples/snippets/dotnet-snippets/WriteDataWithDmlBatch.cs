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

// [START spanner_dml_batch]
using Npgsql;

namespace dotnet_snippets;

public static class WriteDataWithDmlBatchSample
{
    readonly struct Singer
    {
        public Singer(long singerId, string firstName, string lastName)
        {
            SingerId = singerId;
            FirstName = firstName;
            LastName = lastName;
        }

        public long SingerId { get; }
        public string FirstName { get; }
        public string LastName { get; }
    }
    
    public static void WriteDataWithDmlBatch(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        // Add multiple rows in one DML batch.
        const string sql = "INSERT INTO singers (singer_id, first_name, last_name) VALUES ($1, $2, $3)";
        List<Singer> singers =
        [
            new Singer(/* SingerId = */ 16L, "Sarah", "Wilson"),
            new Singer(/* SingerId = */ 17L, "Ethan", "Miller"),
            new Singer(/* SingerId = */ 18L, "Maya", "Patel")
        ];
        using var batch = new NpgsqlBatch(connection);
        foreach (var singer in singers)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand
            {
                CommandText = sql,
                Parameters =
                {
                    new NpgsqlParameter {Value = singer.SingerId},
                    new NpgsqlParameter {Value = singer.FirstName},
                    new NpgsqlParameter {Value = singer.LastName}
                }
            });
        }
        var updateCount = batch.ExecuteNonQuery();
        Console.WriteLine($"{updateCount} records inserted.");
    }
}
// [END spanner_dml_batch]
