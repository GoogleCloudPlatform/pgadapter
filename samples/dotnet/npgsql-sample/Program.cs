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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Npgsql;

namespace npgsql_sample;

internal static class Sample
{
    private static async Task Main(string[] args)
    {
        // Start PGAdapter and the Spanner emulator in a Docker container.
        var pgadapter = await StartPGAdapterAndEmulator();
        
        var connectionString = $"Host=localhost;Port={pgadapter.GetMappedPublicPort(5432)};Database=my-database;SSL Mode=Disable";
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var cmd = new NpgsqlCommand("select 'Hello World!' as hello", connection);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var greeting = reader.GetString(0);
            Console.WriteLine($"Greeting from Cloud Spanner PostgreSQL: {greeting}");
        }
        
        // Stop PGAdapter and the emulator.
        await pgadapter.StopAsync();
    }

    private static async Task<IContainer> StartPGAdapterAndEmulator()
    {
        var container = new ContainerBuilder()
            .WithImagePullPolicy(imagePullPolicy: PullPolicy.Always)
            // Use the PGAdapter image that also contains automatically starts the Spanner emulator.
            .WithImage("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator")
            // Bind port 5432 of the container to a random port on the host.
            .WithPortBinding(5432, true)
            // Wait until the PostgreSQL port is available.
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(5432))
            // Build the container configuration.
            .Build();
        await container.StartAsync();
        return container;
    }
}