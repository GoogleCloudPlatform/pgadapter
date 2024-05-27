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

// [START spanner_read_only_transaction]
using Npgsql;
using System.Data;

namespace dotnet_snippets;

static class ReadOnlyTransactionSample
{
    internal static void ReadOnlyTransaction(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Start a read-only transaction.
        // You must specify Serializable as the isolation level, as the npgsql driver
        // will otherwise automatically set the isolation level to read-committed.
        var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        // This SQL statement instructs the npgsql driver to use
        // a read-only transaction.
        cmd.CommandText = "set transaction read only";
        cmd.ExecuteNonQuery();
        
        cmd.CommandText = "SELECT singer_id, album_id, album_title " +
                          "FROM albums " +
                          "ORDER BY singer_id, album_id";
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine($"{reader["singer_id"]} {reader["album_id"]} {reader["album_title"]}");
            }
        }
        cmd.CommandText = "SELECT singer_id, album_id, album_title "
                          + "FROM albums "
                          + "ORDER BY album_title";
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                Console.WriteLine($"{reader["singer_id"]} {reader["album_id"]} {reader["album_title"]}");
            }
        }
        // End the read-only transaction by calling commit().
        transaction.Commit();
    }
}
// [END spanner_read_only_transaction]
