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

package com.google.example;

public class SampleRunner {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println();
      System.err.println("Invalid number of arguments: " + args.length);
      System.err.println("Usage: SampleRunner <sample-name> <database-name>");
      System.err.println();
      System.exit(1);
    }
    String sample = args[0];
    String database = args[1];
    String host = System.getProperty("pgadapter.host", "localhost");
    int port = Integer.parseInt(System.getProperty("pgadapter.port", "5432"));

    switch (sample) {
      case "createtables":
        CreateTables.createTables(host, port, database);
        break;
      case "createconnection":
        CreateConnection.createConnection(host, port, database);
        break;
      case "writeusingdml":
        WriteDataWithDml.writeDataWithDml(host, port, database);
        break;
      case "writeusingdmlbatch":
        WriteDataWithDmlBatch.writeDataWithDmlBatch(host, port, database);
        break;
      case "write":
        WriteDataWithCopy.writeDataWithCopy(host, port, database);
        break;
      case "query":
        QueryData.queryData(host, port, database);
        break;
      case "querywithparameter":
        QueryDataWithParameter.queryDataWithParameter(host, port, database);
        break;
      case "addmarketingbudget":
        AddColumn.addColumn(host, port, database);
        break;
      case "ddlbatch":
        DdlBatch.ddlBatch(host, port, database);
        break;
      case "update":
        UpdateDataWithCopy.updateDataWithCopy(host, port, database);
        break;
      case "querymarketingbudget":
        QueryDataWithNewColumn.queryDataWithNewColumn(host, port, database);
        break;
      case "writewithtransactionusingdml":
        UpdateDataWithTransaction.writeWithTransactionUsingDml(host, port, database);
        break;
      case "tags":
        Tags.tags(host, port, database);
        break;
      case "readonlytransaction":
        ReadOnlyTransaction.readOnlyTransaction(host, port, database);
        break;
      case "databoost":
        DataBoost.dataBoost(host, port, database);
        break;
      case "partitioneddml":
        PartitionedDml.partitionedDml(host, port, database);
        break;
      default:
        throw new IllegalArgumentException("Unknown sample: " + sample);
    }
  }

}
