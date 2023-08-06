// Copyright 2023 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Npgsql;

var project = Environment.GetEnvironmentVariable("SPANNER_PROJECT") ?? "my-project";
var instance = Environment.GetEnvironmentVariable("SPANNER_INSTANCE") ?? "my-instance";
var database = Environment.GetEnvironmentVariable("SPANNER_DATABASE") ?? "my-database";
var qualifiedDatabaseName = $"projects/{project}/instances/{instance}/databases/{database}";

var pgadapterHost = Environment.GetEnvironmentVariable("PGADAPTER_HOST") ?? "localhost";
var pgadapterPort = Environment.GetEnvironmentVariable("PGADAPTER_PORT") ?? "5432";

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
var url = $"http://0.0.0.0:{port}";

var app = WebApplication.CreateBuilder(args).Build();
app.MapGet("/", HelloWorld);
app.Run(url);

string HelloWorld()
{
    // Use a fully qualified database name when connecting to Cloud Spanner, as PGAdapter
    // was started without a default project and instance.
    var connectionString = $"Host={pgadapterHost};Port={pgadapterPort};Database={qualifiedDatabaseName}";
    using var connection = new NpgsqlConnection(connectionString);
    connection.Open();

    using var cmd = new NpgsqlCommand("select 'Hello world!' as hello", connection);
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        var greeting = reader.GetString(0);
        return $"Greeting from Cloud Spanner PostgreSQL using npgsql: {greeting}{Environment.NewLine}";
    }
    return "No greeting returned by Cloud Spanner";
}