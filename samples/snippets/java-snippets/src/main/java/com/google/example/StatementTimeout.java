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

// [START spanner_statement_timeout]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class StatementTimeout {
  static void queryWithTimeout(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (Statement statement = connection.createStatement()) {
        // JDBC has built-in support for setting query timeouts.
        // This sets the query timeout for this statement to 5 seconds.
        statement.setQueryTimeout(5);
        try (ResultSet resultSet =
            statement
                .executeQuery("SELECT singer_id, album_id, album_title "
                    + "FROM albums "
                    + "WHERE album_title in ("
                    + "  SELECT first_name "
                    + "  FROM singers "
                    + "  WHERE last_name LIKE '%a%'"
                    + "     OR last_name LIKE '%m%'"
                    + ")")) {
          while (resultSet.next()) {
            System.out.printf(
                "%d %d %s\n",
                resultSet.getLong("singer_id"),
                resultSet.getLong("album_id"),
                resultSet.getString("album_title"));
          }
        }
      }
    }
  }
}
// [END spanner_query_data]
