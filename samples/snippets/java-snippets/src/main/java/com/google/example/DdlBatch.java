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

// [START spanner_ddl_batch]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class DdlBatch {
  static void ddlBatch(String host, int port, String database) throws SQLException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      try (Statement statement = connection.createStatement()) {
        // Create two new tables in one batch.
        statement.addBatch(
            "CREATE TABLE venues ("
                + "  venue_id    bigint not null primary key,"
                + "  name        varchar(1024),"
                + "  description jsonb"
                + ")");
        statement.addBatch(
            "CREATE TABLE concerts ("
                + "  concert_id bigint not null primary key ,"
                + "  venue_id   bigint not null,"
                + "  singer_id  bigint not null,"
                + "  start_time timestamptz,"
                + "  end_time   timestamptz,"
                + "  constraint fk_concerts_venues foreign key"
                + "    (venue_id) references venues (venue_id),"
                + "  constraint fk_concerts_singers foreign key"
                + "    (singer_id) references singers (singer_id)"
                + ")");
        statement.executeBatch();
      }
      System.out.println("Added venues and concerts tables");
    }
  }
}
// [END spanner_ddl_batch]
