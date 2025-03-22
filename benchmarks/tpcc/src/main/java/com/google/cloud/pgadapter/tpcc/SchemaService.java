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
package com.google.cloud.pgadapter.tpcc;

import com.google.cloud.spanner.Dialect;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaService {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);

  private final String connectionUrl;

  private final Dialect dialect;

  SchemaService(String connectionUrl, Dialect dialect) {
    this.connectionUrl = connectionUrl;
    this.dialect = dialect;
  }

  void createSchema() throws IOException, SQLException {
    LOG.info("Schema connection URL: " + connectionUrl);
    boolean isGoogleSQL = dialect == Dialect.GOOGLE_STANDARD_SQL;
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Check if the tables already exist.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select count(1) "
                      + "from information_schema.tables "
                      + "where "
                      + (isGoogleSQL ? "table_schema='' and " : "table_schema='public' and ")
                      + "table_name in ('warehouse', 'district', 'customer', 'history', 'orders', 'new_orders', 'order_line', 'stock', 'item')")) {
        if (resultSet.next() && resultSet.getInt(1) == 9) {
          LOG.info("Skipping schema creation as tables already exist");
          return;
        }
      }

      URL url =
          AbstractBenchmarkRunner.class.getResource(
              isGoogleSQL ? "/schema_googlesql.sql" : "/schema.sql");
      Path path = Paths.get(Objects.requireNonNull(url).getPath());
      String ddl = Files.readString(path);
      LOG.info("Executing schema statements");
      String[] statements = ddl.trim().split(";");
      for (String statement : statements) {
        LOG.info("Statement: " + statement);
        connection.createStatement().execute(statement);
      }
    }
  }
}
