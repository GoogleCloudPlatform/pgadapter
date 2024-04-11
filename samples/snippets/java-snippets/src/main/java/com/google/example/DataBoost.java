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

// [START spanner_data_boost]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

class DataBoost {
  static void dataBoost(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // This enables Data Boost for all partitioned queries on this connection.
      connection
          .createStatement()
          .execute("set spanner.data_boost_enabled=true");

      // Run a partitioned query. This query will use Data Boost.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "run partitioned query "
                      + "select singer_id, first_name, last_name "
                      + "from singers")) {
        while (resultSet.next()) {
          System.out.printf(
              "%d %s %s\n",
              resultSet.getLong("singer_id"),
              resultSet.getString("first_name"),
              resultSet.getString("last_name"));
        }
      }
    }
  }
}
// [END spanner_data_boost]
