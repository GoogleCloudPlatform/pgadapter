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
using NUnit.Framework;

namespace dotnet_snippets;

[TestFixture]
public class SampleTests
{
    private const string Database = "example-db";
    private IContainer _container;
    
    [OneTimeSetUp]
    public async Task StartEmulator()
    {
        _container = await StartPGAdapterAndEmulator();
    }

    [OneTimeTearDown]
    public async Task StopEmulator()
    {
        await _container.StopAsync();
    }

    string CaptureOutput(Action action)
    {
        var original = Console.Out;
        try
        {
            using var sw = new StringWriter();
            Console.SetOut(sw);
            action.Invoke();
            return sw.ToString();
        }
        finally
        {
            Console.SetOut(original);
        }
    }
    
    [Test, Order(1)]
    public void CreateTables()
    {
        var output = CaptureOutput(() =>
        CreateTablesSample.CreateTables(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"Created Singers & Albums tables in database: [{Database}]{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(2)]
    public void CreateConnection()
    {
        var output = CaptureOutput(() =>
            CreateConnectionSample.CreateConnection(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"Greeting from Cloud Spanner PostgreSQL: Hello World!{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
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