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

// [START spanner_read_only_transaction]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

class ReadOnlyTransaction {
  static void readOnlyTransaction(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Set AutoCommit=false to enable transactions.
      connection.setAutoCommit(false);
      // This SQL statement instructs the JDBC driver to use
      // a read-only transaction.
      connection.createStatement().execute("set transaction read only");

      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "SELECT singer_id, album_id, album_title "
                      + "FROM albums "
                      + "ORDER BY singer_id, album_id")) {
        while (resultSet.next()) {
          System.out.printf(
              "%d %d %s\n",
              resultSet.getLong("singer_id"),
              resultSet.getLong("album_id"),
              resultSet.getString("album_title"));
        }
      }
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "SELECT singer_id, album_id, album_title "
                      + "FROM albums "
                      + "ORDER BY album_title")) {
        while (resultSet.next()) {
          System.out.printf(
              "%d %d %s\n",
              resultSet.getLong("singer_id"),
              resultSet.getLong("album_id"),
              resultSet.getString("album_title"));
        }
      }
      // End the read-only transaction by calling commit().
      connection.commit();
    }
  }
}
// [END spanner_read_only_transaction]
