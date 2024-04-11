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

// [START spanner_copy_from_stdin]
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

class WriteDataWithCopy {
  
  static void writeDataWithCopy(String host, int port, String database)
      throws SQLException, IOException {
    String connectionUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Unwrap the PostgreSQL JDBC connection interface to get access to
      // a CopyManager.
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      
      // Enable 'partitioned_non_atomic' mode. This ensures that the COPY operation
      // will succeed even if it exceeds Spanner's mutation limit per transaction.
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");
      long numSingers = copyManager.copyIn(
          "COPY singers (singer_id, first_name, last_name) FROM STDIN",
          WriteDataWithCopy.class.getResourceAsStream("singers_data.txt"));
      System.out.printf("Copied %d singers\n", numSingers);
      
      long numAlbums = copyManager.copyIn(
          "COPY albums FROM STDIN",
          WriteDataWithCopy.class.getResourceAsStream("albums_data.txt"));
      System.out.printf("Copied %d albums\n", numAlbums);
    }
  }
}
// [END spanner_copy_from_stdin]
