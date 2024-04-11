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

// [START spanner_transaction_and_statement_tag]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class Tags {
  
  static void tags(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Set AutoCommit=false to enable transactions.
      connection.setAutoCommit(false);
      // Set the TRANSACTION_TAG session variable to set a transaction tag
      // for the current transaction.
      connection
          .createStatement()
          .execute("set spanner.transaction_tag='example-tx-tag'");

      // Set the STATEMENT_TAG session variable to set the request tag
      // that should be included with the next SQL statement.
      connection
          .createStatement()
          .execute("set spanner.statement_tag='query-marketing-budget'");
      long marketingBudget = 0L;
      long singerId = 1L;
      long albumId = 1L;
      try (PreparedStatement statement = connection.prepareStatement(
          "select marketing_budget "
              + "from albums "
              + "where singer_id=? and album_id=?")) {
        statement.setLong(1, singerId);
        statement.setLong(2, albumId);
        try (ResultSet albumResultSet = statement.executeQuery()) {
          while (albumResultSet.next()) {
            marketingBudget = albumResultSet.getLong(1);
          }
        }
      }
      // Reduce the marketing budget by 10% if it is more than 1,000.
      final long maxMarketingBudget = 1000L;
      final float reduction = 0.1f;
      if (marketingBudget > maxMarketingBudget) {
        marketingBudget -= (long) (marketingBudget * reduction);
        connection
            .createStatement()
            .execute("set spanner.statement_tag='reduce-marketing-budget'");
        try (PreparedStatement statement = connection.prepareStatement(
            "update albums set marketing_budget=? "
                + "where singer_id=? AND album_id=?")) {
          int paramIndex = 0;
          statement.setLong(++paramIndex, marketingBudget);
          statement.setLong(++paramIndex, singerId);
          statement.setLong(++paramIndex, albumId);
          statement.executeUpdate();
        }
      }

      // Commit the current transaction.
      connection.commit();
      System.out.println("Reduced marketing budget");
    }
  }
}
// [END spanner_transaction_and_statement_tag]
