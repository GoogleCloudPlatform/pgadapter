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

using dotnet_snippets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace sample_tests;

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
    
    [Test, Order(3)]
    public void WriteDataWithDml()
    {
        var output = CaptureOutput(() =>
            WriteDataWithDmlSample.WriteDataWithDml(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"4 records inserted.{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(4)]
    public void WriteDataWithDmlBatch()
    {
        var output = CaptureOutput(() =>
            WriteDataWithDmlBatchSample.WriteDataWithDmlBatch(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"3 records inserted.{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(5)]
    public void WriteDataWithCopy()
    {
        var output = CaptureOutput(() =>
            WriteDataWithCopySample.WriteDataWithCopy(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"Copied 5 singers{Environment.NewLine}Copied 5 albums{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(6)]
    public void QueryData()
    {
        var output = CaptureOutput(() =>
            QueryDataSample.QueryData(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"1 2 Go, Go, Go{Environment.NewLine}" +
                       $"2 2 Forever Hold Your Peace{Environment.NewLine}" +
                       $"1 1 Total Junk{Environment.NewLine}" +
                       $"2 1 Green{Environment.NewLine}" +
                       $"2 3 Terrified{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(7)]
    public void QueryDataWithParameter()
    {
        var output = CaptureOutput(() =>
            QueryDataWithParameterSample.QueryDataWithParameter(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        var expected = $"12 Melissa Garcia{Environment.NewLine}";
        Assert.That(output, Is.EqualTo(expected));
    }
    
    [Test, Order(8)]
    public void QueryWithTimeout()
    {
        var output = CaptureOutput(() =>
            QueryWithTimeoutSample.QueryWithTimeout(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo(""));
    }
    
    [Test, Order(9)]
    public void AddColumn()
    {
        var output = CaptureOutput(() =>
            AddColumnSample.AddColumn(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Added marketing_budget column{Environment.NewLine}"));
    }
    
    [Test, Order(10)]
    public void DdlBatch()
    {
        var output = CaptureOutput(() =>
            DdlBatchSample.DdlBatch(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Added venues and concerts tables{Environment.NewLine}"));
    }
    
    [Test, Order(11)]
    public void UpdateDataWithCopy()
    {
        var output = CaptureOutput(() =>
            UpdateDataWithCopySample.UpdateDataWithCopy(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Updated 2 albums{Environment.NewLine}"));
    }
    
    [Test, Order(12)]
    public void QueryDataWithNewColumn()
    {
        var output = CaptureOutput(() =>
            QueryDataWithNewColumnSample.QueryWithNewColumnData(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"1 1 100000{Environment.NewLine}" +
                                       $"1 2 {Environment.NewLine}" +
                                       $"2 1 {Environment.NewLine}" +
                                       $"2 2 500000{Environment.NewLine}" +
                                       $"2 3 {Environment.NewLine}"));
    }
    
    [Test, Order(13)]
    public void WriteDataWithTransaction()
    {
        var output = CaptureOutput(() =>
            UpdateDataWithTransactionSample.WriteWithTransactionUsingDml(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Transferred marketing budget from Album 2 to Album 1{Environment.NewLine}"));
    }
    
    [Test, Order(14)]
    public void Tags()
    {
        var output = CaptureOutput(() =>
            TagsSample.Tags(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Reduced marketing budget{Environment.NewLine}"));
    }
    
    [Test, Order(15)]
    public void ReadOnlyTransaction()
    {
        var output = CaptureOutput(() =>
            ReadOnlyTransactionSample.ReadOnlyTransaction(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"1 1 Total Junk{Environment.NewLine}"
                                       + $"1 2 Go, Go, Go{Environment.NewLine}"
                                       + $"2 1 Green{Environment.NewLine}"
                                       + $"2 2 Forever Hold Your Peace{Environment.NewLine}"
                                       + $"2 3 Terrified{Environment.NewLine}"
                                       + $"2 2 Forever Hold Your Peace{Environment.NewLine}"
                                       + $"1 2 Go, Go, Go{Environment.NewLine}"
                                       + $"2 1 Green{Environment.NewLine}"
                                       + $"2 3 Terrified{Environment.NewLine}"
                                       + $"1 1 Total Junk{Environment.NewLine}"));
    }
    
    [Test, Order(16)]
    public void DataBoost()
    {
        var output = CaptureOutput(() =>
            DataBoostSample.DataBoost(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"2 Catalina Smith{Environment.NewLine}"
                                       + $"4 Lea Martin{Environment.NewLine}"
                                       + $"12 Melissa Garcia{Environment.NewLine}"
                                       + $"14 Jacqueline Long{Environment.NewLine}"
                                       + $"16 Sarah Wilson{Environment.NewLine}"
                                       + $"18 Maya Patel{Environment.NewLine}"
                                       + $"1 Marc Richards{Environment.NewLine}"
                                       + $"3 Alice Trentor{Environment.NewLine}"
                                       + $"5 David Lomond{Environment.NewLine}"
                                       + $"13 Russel Morales{Environment.NewLine}"
                                       + $"15 Dylan Shaw{Environment.NewLine}"
                                       + $"17 Ethan Miller{Environment.NewLine}"));
    }
        
    [Test, Order(17)]
    public void PartitionedDml()
    {
        var output = CaptureOutput(() =>
            PartitionedDmlSample.PartitionedDml(_container.Hostname, _container.GetMappedPublicPort(5432), Database));
        Assert.That(output, Is.EqualTo($"Updated at least 3 albums{Environment.NewLine}"));
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