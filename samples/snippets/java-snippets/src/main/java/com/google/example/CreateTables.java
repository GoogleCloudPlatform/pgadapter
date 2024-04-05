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

// [START spanner_create_database]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class CreateTables {
  static void createTables(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (Statement statement = connection.createStatement()) {
        // Create two tables in one batch.
        statement.addBatch(
            "CREATE TABLE Singers ("
                + "  SingerId   bigint NOT NULL,"
                + "  FirstName  character varying(1024),"
                + "  LastName   character varying(1024),"
                + "  SingerInfo bytea,"
                + "  FullName character varying(2048) GENERATED "
                + "  ALWAYS AS (FirstName || ' ' || LastName) STORED,"
                + "  PRIMARY KEY (SingerId)"
                + ")");
        statement.addBatch(
            "CREATE TABLE Albums ("
                + "  SingerId     bigint NOT NULL,"
                + "  AlbumId      bigint NOT NULL,"
                + "  AlbumTitle   character varying(1024),"
                + "  PRIMARY KEY (SingerId, AlbumId)"
                + ") INTERLEAVE IN PARENT Singers ON DELETE CASCADE");
        statement.executeBatch();
        System.out.println("Created Singers & Albums tables in database: [" + database + "]");
      }
    }
  }
}
// [END spanner_create_database]
