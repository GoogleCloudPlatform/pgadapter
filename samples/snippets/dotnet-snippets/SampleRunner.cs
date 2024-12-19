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

if (args.Length < 2 || args.Length > 3)
{
    Console.WriteLine();
    Console.WriteLine($"Invalid number of arguments: {args.Length}");
    Console.WriteLine("Usage: SampleRunner <sample-name> <database-name> [host:port]");
    Console.WriteLine();
    Environment.Exit(1);
}

var sample = args[0];
var database = args[1];
var hostPort = args.Length == 3 ? args[2] : "localhost:5432";
if (!hostPort.Contains(':'))
{
    Console.WriteLine("Missing ':' in host:port argument");
    Environment.Exit(1);
}

var host = hostPort.Split(':')[0];
var port = int.Parse(hostPort.Split(':')[1]);

switch (sample)
{
    case "createtables":
        CreateTablesSample.CreateTables(host, port, database);
        break;
    case "createconnection":
        CreateConnectionSample.CreateConnection(host, port, database);
        break;
    case "writeusingdml":
        WriteDataWithDmlSample.WriteDataWithDml(host, port, database);
        break;
    case "writeusingdmlbatch":
        WriteDataWithDmlBatchSample.WriteDataWithDmlBatch(host, port, database);
        break;
    case "write":
        WriteDataWithCopySample.WriteDataWithCopy(host, port, database);
        break;
    case "query":
        QueryDataSample.QueryData(host, port, database);
        break;
    case "querywithparameter":
        QueryDataWithParameterSample.QueryDataWithParameter(host, port, database);
        break;
    case "addmarketingbudget":
        AddColumnSample.AddColumn(host, port, database);
        break;
    case "ddlbatch":
        DdlBatchSample.DdlBatch(host, port, database);
        break;
    case "update":
        UpdateDataWithCopySample.UpdateDataWithCopy(host, port, database);
        break;
    case "querymarketingbudget":
        QueryDataWithNewColumnSample.QueryWithNewColumnData(host, port, database);
        break;
    case "writewithtransactionusingdml":
        UpdateDataWithTransactionSample.WriteWithTransactionUsingDml(host, port, database);
        break;
    case "tags":
        TagsSample.Tags(host, port, database);
        break;
    case "readonlytransaction":
        ReadOnlyTransactionSample.ReadOnlyTransaction(host, port, database);
        break;
    case "databoost":
        DataBoostSample.DataBoost(host, port, database);
        break;
    case "partitioneddml":
        PartitionedDmlSample.PartitionedDml(host, port, database);
        break;
    default:
        Console.WriteLine($"Unknown sample name: {sample}");
        Environment.Exit(1);
        break;
}
