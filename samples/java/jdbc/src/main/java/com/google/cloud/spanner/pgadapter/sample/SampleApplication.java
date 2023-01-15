// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SampleApplication {
  static {
    try {
      Class.forName(org.postgresql.Driver.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load JDBC driver: " + e.getMessage(), e);
    }
  }

  public static void main(String[] args) throws SQLException {
    String connectionUrl =
        System.getProperty("connection_url", "jdbc:postgresql://localhost:5432/my-database");
    runSample(connectionUrl);
  }

  static void runSample(String connectionUrl) throws SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select 'Hello World!' as greeting")) {
        while (resultSet.next()) {
          System.out.printf("Greeting: %s\n", resultSet.getString("greeting"));
        }
      }
    }
  }
}
