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

// [START spanner_dml_getting_started_update]
using Npgsql;
using System.Data;

namespace dotnet_snippets;

static class UpdateDataWithTransactionSample
{
    internal static void WriteWithTransactionUsingDml(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        
        // Start a transaction with isolation level Serializable.
        // Spanner only supports this isolation level. Trying to use a lower
        // isolation level (including the default isolation level READ COMMITTED),
        // will result in an error.
        var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        
        // Transfer marketing budget from one album to another. We do it in a
        // transaction to ensure that the transfer is atomic.
        const string selectMarketingBudgetSql =
            "SELECT marketing_budget from albums WHERE singer_id = $1 and album_id = $2";

        using var selectMarketingBudgetCommand = connection.CreateCommand();
        selectMarketingBudgetCommand.Transaction = transaction;
        selectMarketingBudgetCommand.CommandText = selectMarketingBudgetSql;
        
        // Select the marketing_budget of the Album with SingerId=2 and AlbumId=2.
        selectMarketingBudgetCommand.Parameters.Add(new NpgsqlParameter {Value = 2L});
        selectMarketingBudgetCommand.Parameters.Add(new NpgsqlParameter {Value = 2L});
        var album2Budget = (long?) selectMarketingBudgetCommand.ExecuteScalar();
        
        // The transaction will only be committed if this condition still holds
        // at the time of commit. Otherwise, the transaction will be aborted.
        const long transfer = 200000;
        if (album2Budget >= transfer)
        {
            // Re-use the existing command for selecting the
            // marketing_budget to get the budget for Album 1.
            // Bind the query parameters to SingerId=1 and AlbumId=1.
            selectMarketingBudgetCommand.Parameters.Clear();
            selectMarketingBudgetCommand.Parameters.Add(new NpgsqlParameter {Value = 1L});
            selectMarketingBudgetCommand.Parameters.Add(new NpgsqlParameter {Value = 1L});
            var album1Budget = (long?) selectMarketingBudgetCommand.ExecuteScalar();
            
            // Transfer part of the marketing budget of Album 2 to Album 1.
            album1Budget += transfer;
            album2Budget -= transfer;
            const string updateSql = "UPDATE albums " +
                                     "SET marketing_budget = $1 " +
                                     "WHERE singer_id = $2 " +
                                     "and album_id = $3";
            // Update both albums in one batch.
            using var batch = connection.CreateBatch();
            batch.Transaction = transaction;
            batch.BatchCommands.Add(new NpgsqlBatchCommand
            {
                CommandText = updateSql,
                Parameters =
                {
                    new NpgsqlParameter {Value = album1Budget},
                    new NpgsqlParameter {Value = 1L},
                    new NpgsqlParameter {Value = 1L}
                }
            });
            batch.BatchCommands.Add(new NpgsqlBatchCommand
            {
                CommandText = updateSql,
                Parameters =
                {
                    new NpgsqlParameter {Value = album2Budget},
                    new NpgsqlParameter {Value = 2L},
                    new NpgsqlParameter {Value = 2L}
                }
            });
            // Execute both DML statements in one batch.
            batch.ExecuteNonQuery();
        }
        // Commit the transaction.
        transaction.Commit();
        Console.WriteLine("Transferred marketing budget from Album 2 to Album 1");
    }
}
// [END spanner_dml_getting_started_update]
