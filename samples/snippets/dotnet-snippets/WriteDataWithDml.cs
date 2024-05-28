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

// [START spanner_dml_getting_started_insert]
using Npgsql;

namespace dotnet_snippets;

public static class WriteDataWithDmlSample
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
    
    public static void WriteDataWithDml(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        // Add 4 rows in one statement.
        using var cmd = new NpgsqlCommand("INSERT INTO singers (singer_id, first_name, last_name) VALUES "
                                          + "($1, $2, $3), "
                                          + "($4, $5, $6), "
                                          + "($7, $8, $9), "
                                          + "($10, $11, $12)", connection);
        List<Singer> singers =
        [
            new Singer(/* SingerId = */ 12L, "Melissa", "Garcia"),
            new Singer(/* SingerId = */ 13L, "Russel", "Morales"),
            new Singer(/* SingerId = */ 14L, "Jacqueline", "Long"),
            new Singer(/* SingerId = */ 15L, "Dylan", "Shaw")
        ];
        foreach (var singer in singers)
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = singer.SingerId });
            cmd.Parameters.Add(new NpgsqlParameter { Value = singer.FirstName });
            cmd.Parameters.Add(new NpgsqlParameter { Value = singer.LastName });
        }
        var updateCount = cmd.ExecuteNonQuery();
        Console.WriteLine($"{updateCount} records inserted.");
    }
}
// [END spanner_dml_getting_started_insert]
