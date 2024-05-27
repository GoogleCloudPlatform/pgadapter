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

// [START spanner_transaction_and_statement_tag]
using Npgsql;
using System.Data;

namespace dotnet_snippets;

static class TagsSample
{
    internal static void Tags(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Start a transaction with isolation level Serializable.
        // Spanner only supports this isolation level. Trying to use a lower
        // isolation level (including the default isolation level READ COMMITTED),
        // will result in an error.
        var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        // Create a command that uses the current transaction.
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        
        // Set the TRANSACTION_TAG session variable to set a transaction tag
        // for the current transaction.
        cmd.CommandText = "set spanner.transaction_tag='example-tx-tag'";
        cmd.ExecuteNonQuery();

        // Set the STATEMENT_TAG session variable to set the request tag
        // that should be included with the next SQL statement.
        cmd.CommandText = "set spanner.statement_tag='query-marketing-budget'";
        cmd.ExecuteNonQuery();

        // Get the marketing_budget of Album (1,1).
        cmd.CommandText = "select marketing_budget from albums where singer_id=$1 and album_id=$2";
        cmd.Parameters.Add(new NpgsqlParameter { Value = 1L });
        cmd.Parameters.Add(new NpgsqlParameter { Value = 1L });
        var marketingBudget = (long?)cmd.ExecuteScalar();

        // Reduce the marketing budget by 10% if it is more than 1,000.
        if (marketingBudget > 1000L)
        {
            marketingBudget -= (long) (marketingBudget * 0.1);
            
            // Set the statement tag to use for the update statement.
            cmd.Parameters.Clear();
            cmd.CommandText = "set spanner.statement_tag='reduce-marketing-budget'";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "update albums set marketing_budget=$1 where singer_id=$2 AND album_id=$3";
            cmd.Parameters.Add(new NpgsqlParameter { Value = marketingBudget });
            cmd.Parameters.Add(new NpgsqlParameter { Value = 1L });
            cmd.Parameters.Add(new NpgsqlParameter { Value = 1L });
            cmd.ExecuteNonQuery();
        }

        // Commit the current transaction.
        transaction.Commit();
        Console.WriteLine("Reduced marketing budget");
    }
}
// [END spanner_transaction_and_statement_tag]
