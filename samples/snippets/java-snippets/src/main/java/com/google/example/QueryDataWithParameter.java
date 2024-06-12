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

// [START spanner_query_with_parameter]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class QueryDataWithParameter {
  static void queryDataWithParameter(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "SELECT singer_id, first_name, last_name "
                  + "FROM singers "
                  + "WHERE last_name = ?")) {
        statement.setString(1, "Garcia");
        try (ResultSet resultSet = statement.executeQuery()) {
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
}
// [END spanner_query_data]
