package com.google.cloud.pgadapter.benchmark;

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

  SchemaService(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  void createSchema() throws IOException, SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl)) {
      // Check if the tables already exist.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select count(1) "
                      + "from information_schema.tables "
                      + "where table_schema='public' "
                      + "and table_name in ('benchmark_all_types', 'benchmark_results')")) {
        if (resultSet.next() && resultSet.getInt(1) == 2) {
          LOG.info("Skipping schema creation as tables already exist");
          return;
        }
      }

      URL url = JdbcBenchmarkRunner.class.getResource("/schema.sql");
      Path path = Paths.get(Objects.requireNonNull(url).getPath());
      String ddl = Files.readString(path);
      LOG.info("Executing schema statements");
      String[] statements = ddl.split(";");
      for (String statement : statements) {
        LOG.info(statement);
        connection.createStatement().execute(statement);
      }
    }
  }
}
