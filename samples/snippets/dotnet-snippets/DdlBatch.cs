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

// [START spanner_ddl_batch]
using Npgsql;

namespace dotnet_snippets;

public static class DdlBatchSample
{
    public static void DdlBatch(string host, int port, string database)
    {
        var connectionString = $"Host={host};Port={port};Database={database};SSL Mode=Disable;Pooling=False";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        // Create two new tables in one batch.
        var batch = connection.CreateBatch();
        batch.BatchCommands.Add(new NpgsqlBatchCommand(
            "CREATE TABLE venues ("
            + "  venue_id    bigint not null primary key,"
            + "  name        varchar(1024),"
            + "  description jsonb"
            + ")"));
        batch.BatchCommands.Add(new NpgsqlBatchCommand(
            "CREATE TABLE concerts ("
            + "  concert_id bigint not null primary key ,"
            + "  venue_id   bigint not null,"
            + "  singer_id  bigint not null,"
            + "  start_time timestamptz,"
            + "  end_time   timestamptz,"
            + "  constraint fk_concerts_venues foreign key"
            + "    (venue_id) references venues (venue_id),"
            + "  constraint fk_concerts_singers foreign key"
            + "    (singer_id) references singers (singer_id)"
            + ")"));
        batch.ExecuteNonQuery();
        Console.WriteLine("Added venues and concerts tables");
    }
}
// [END spanner_ddl_batch]
