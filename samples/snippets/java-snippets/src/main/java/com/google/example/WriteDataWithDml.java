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

// [START spanner_dml_getting_started_insert]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

class WriteDataWithDml {
  static class Singer {
    private final long singerId;
    private final String firstName;
    private final String lastName;

    Singer(final long id, final String first, final String last) {
      this.singerId = id;
      this.firstName = first;
      this.lastName = last;
    }
  }
  
  static void writeDataWithDml(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Add 4 rows in one statement.
      // JDBC always uses '?' as a parameter placeholder.
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              "INSERT INTO singers (singer_id, first_name, last_name) VALUES "
                  + "(?, ?, ?), "
                  + "(?, ?, ?), "
                  + "(?, ?, ?), "
                  + "(?, ?, ?)")) {

        final List<Singer> singers =
            Arrays.asList(
                new Singer(/* SingerId = */ 12L, "Melissa", "Garcia"),
                new Singer(/* SingerId = */ 13L, "Russel", "Morales"),
                new Singer(/* SingerId = */ 14L, "Jacqueline", "Long"),
                new Singer(/* SingerId = */ 15L, "Dylan", "Shaw"));

        // Note that JDBC parameters start at index 1.
        int paramIndex = 0;
        for (Singer singer : singers) {
          preparedStatement.setLong(++paramIndex, singer.singerId);
          preparedStatement.setString(++paramIndex, singer.firstName);
          preparedStatement.setString(++paramIndex, singer.lastName);
        }

        int updateCount = preparedStatement.executeUpdate();
        System.out.printf("%d records inserted.\n", updateCount);
      }
    }
  }
}
// [END spanner_dml_getting_started_insert]
