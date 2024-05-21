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

// [START spanner_dml_getting_started_update]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class UpdateDataWithTransaction {

  static void writeWithTransactionUsingDml(String host, int port, String database)
      throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Set AutoCommit=false to enable transactions.
      connection.setAutoCommit(false);

      // Transfer marketing budget from one album to another. We do it in a
      // transaction to ensure that the transfer is atomic. There is no need
      // to explicitly start the transaction. The first statement on the
      // connection will start a transaction when AutoCommit=false.
      String selectMarketingBudgetSql =
          "SELECT marketing_budget from albums WHERE singer_id = ? and album_id = ?";
      long album2Budget = 0;
      try (PreparedStatement selectMarketingBudgetStatement =
          connection.prepareStatement(selectMarketingBudgetSql)) {
        // Bind the query parameters to SingerId=2 and AlbumId=2.
        selectMarketingBudgetStatement.setLong(1, 2);
        selectMarketingBudgetStatement.setLong(2, 2);
        try (ResultSet resultSet = selectMarketingBudgetStatement.executeQuery()) {
          while (resultSet.next()) {
            album2Budget = resultSet.getLong("marketing_budget");
          }
        }
        // The transaction will only be committed if this condition still holds
        // at the time of commit. Otherwise, the transaction will be aborted.
        final long transfer = 200000;
        if (album2Budget >= transfer) {
          long album1Budget = 0;
          // Re-use the existing PreparedStatement for selecting the
          // marketing_budget to get the budget for Album 1.
          // Bind the query parameters to SingerId=1 and AlbumId=1.
          selectMarketingBudgetStatement.setLong(1, 1);
          selectMarketingBudgetStatement.setLong(2, 1);
          try (ResultSet resultSet = selectMarketingBudgetStatement.executeQuery()) {
            while (resultSet.next()) {
              album1Budget = resultSet.getLong("marketing_budget");
            }
          }

          // Transfer part of the marketing budget of Album 2 to Album 1.
          album1Budget += transfer;
          album2Budget -= transfer;
          String updateSql =
              "UPDATE albums "
                  + "SET marketing_budget = ? "
                  + "WHERE singer_id = ? and album_id = ?";
          try (PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {
            // Update Album 1.
            int paramIndex = 0;
            updateStatement.setLong(++paramIndex, album1Budget);
            updateStatement.setLong(++paramIndex, 1);
            updateStatement.setLong(++paramIndex, 1);
            // Create a DML batch by calling addBatch
            // on the current PreparedStatement.
            updateStatement.addBatch();

            // Update Album 2 in the same DML batch.
            paramIndex = 0;
            updateStatement.setLong(++paramIndex, album2Budget);
            updateStatement.setLong(++paramIndex, 2);
            updateStatement.setLong(++paramIndex, 2);
            updateStatement.addBatch();

            // Execute both DML statements in one batch.
            updateStatement.executeBatch();
          }
        }
      }
      // Commit the current transaction.
      connection.commit();
      System.out.println("Transferred marketing budget from Album 2 to Album 1");
    }
  }
}
// [END spanner_dml_getting_started_update]
