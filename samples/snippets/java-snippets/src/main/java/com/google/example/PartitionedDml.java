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

// [START spanner_partitioned_dml]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class PartitionedDml {
  
  static void partitionedDml(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Enable Partitioned DML on this connection.
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
      // Back-fill a default value for the MarketingBudget column.
      long lowerBoundUpdateCount =
          connection
              .createStatement()
              .executeUpdate("update albums "
                  + "set marketing_budget=0 "
                  + "where marketing_budget is null");
      System.out.printf("Updated at least %d albums\n", lowerBoundUpdateCount);
    }
  }
}
// [END spanner_partitioned_dml]
