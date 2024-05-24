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

// [START spanner_create_database]
using Npgsql;

namespace dotnet_snippets;

static class CreateTablesSample
{
    internal static void CreateTables(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        // Create two tables in one batch.
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add(new NpgsqlBatchCommand(
            "create table singers ("
            + "  singer_id   bigint primary key not null,"
            + "  first_name  varchar(1024),"
            + "  last_name   varchar(1024),"
            + "  singer_info bytea,"
            + "  full_name   varchar(2048) generated always as (\n"
            + "      case when first_name is null then last_name\n"
            + "          when last_name  is null then first_name\n"
            + "          else first_name || ' ' || last_name\n"
            + "      end) stored"
            + ")"));
        batch.BatchCommands.Add(new NpgsqlBatchCommand(
            "create table albums ("
            + "  singer_id     bigint not null,"
            + "  album_id      bigint not null,"
            + "  album_title   varchar,"
            + "  primary key (singer_id, album_id)"
            + ") interleave in parent singers on delete cascade"));
        batch.ExecuteNonQuery();
        Console.WriteLine($"Created Singers & Albums tables in database: [{database}]");
    }
}
// [END spanner_create_database]
