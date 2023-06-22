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

package com.google.cloud.spanner.pgadapter.liquibase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ITLiquibaseTest {
  private static final Logger LOGGER = Logger.getLogger(ITLiquibaseTest.class.getName());
  private static final String LIQUIBASE_SAMPLE_DIRECTORY = "samples/java/liquibase";
  private static final String LIQUIBASE_PROPERTIES_FILE =
      LIQUIBASE_SAMPLE_DIRECTORY + "/liquibase.properties";
  private static final String LIQUIBASE_DB_CHANGELOG_DDL_FILE =
      LIQUIBASE_SAMPLE_DIRECTORY + "/create_database_change_log.sql";
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;
  private static String originalLiquibaseProperties;

  @BeforeClass
  public static void setup() throws ClassNotFoundException, IOException, SQLException {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServer(ImmutableList.of());
    // Create databasechangelog and databasechangeloglock tables.
    //    StringBuilder builder = new StringBuilder();
    //    try (Scanner scanner = new Scanner(new FileReader(LIQUIBASE_DB_CHANGELOG_DDL_FILE))) {
    //      while (scanner.hasNextLine()) {
    //        builder.append(scanner.nextLine()).append("\n");
    //      }
    //    }
    //    // Note: We know that all semicolons in this file are outside of literals etc.
    //    String[] ddl = builder.toString().split(";");
    //    String url =
    //        String.format(
    //
    // "jdbc:postgresql://localhost:%d/%s?options=-c%%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction",
    //            testEnv.getPGAdapterPort(), database.getId().getDatabase());
    //    try (Connection connection = DriverManager.getConnection(url)) {
    //      try (Statement statement = connection.createStatement()) {
    //        for (String sql : ddl) {
    //          LOGGER.info("Executing " + sql);
    //          statement.execute(sql);
    //        }
    //      }
    //    }

    // Write liquibase.properties
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(LIQUIBASE_PROPERTIES_FILE))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    originalLiquibaseProperties = original.toString();
    try (FileWriter writer = new FileWriter(LIQUIBASE_PROPERTIES_FILE)) {
      String properties =
          String.format(
              "changeLogFile: dbchangelog.xml\n"
                  + "url: jdbc:postgresql://localhost:%d/%s"
                  + "?options=-c%%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction\n",
              testEnv.getPGAdapterPort(), database.getId().getDatabase());
      LOGGER.info("Using Liquibase properties:\n" + properties);
      writer.write(properties);
      writer.flush();
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    try (FileWriter writer = new FileWriter(LIQUIBASE_PROPERTIES_FILE)) {
      writer.write(originalLiquibaseProperties);
    }
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testLiquibaseUpdate() throws IOException, InterruptedException, SQLException {
    // Validate the Liquibase changelog.
    runLiquibaseCommand("liquibase:validate");
    // Verify that there is a rollback available for each change set.
    runLiquibaseCommand("liquibase:futureRollbackSQL");
    // Update the database with all defined change sets up to v3.3.
    runLiquibaseCommand("liquibase:update", "-Dliquibase.toTag=v3.3");

    // Verify that all tables were created and all data was loaded.
    String url =
        String.format(
            "jdbc:postgresql://localhost:%d/%s",
            testEnv.getPGAdapterPort(), database.getId().getDatabase());
    try (Connection connection = DriverManager.getConnection(url)) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select table_name from information_schema.tables where table_schema='public' order by table_name")) {
        assertTrue(resultSet.next());
        assertEquals("albums", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("all_types", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("concerts", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("databasechangelog", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("databasechangeloglock", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("singers", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("singers_by_last_name", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("tracks", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("venues", resultSet.getString(1));

        assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select start_time from concerts order by start_time nulls first")) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getTimestamp(1));
        assertTrue(resultSet.next());
        assertEquals(
            Timestamp.parseTimestamp("2022-08-31T18:30:00Z").toSqlTimestamp(),
            resultSet.getTimestamp(1));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from concerts")) {
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }

      // Apply all remaining change sets.
      runLiquibaseCommand("liquibase:update");
      // The singers table should now contain an 'address' column that has a null value for all
      // rows.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select count(*) from singers where address is null")) {
        assertTrue(resultSet.next());
        assertEquals(5L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Rollback to v3.1.
      runLiquibaseCommand("liquibase:rollback", "-Dliquibase.rollbackTag=v3.1");
      // Verify that the data in the concerts table was removed.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from concerts")) {
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }

      // Rollback everything.
      runLiquibaseCommand("liquibase:rollback", "-Dliquibase.rollbackTag=v0.0");
      // Verify that all tables have been removed.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select table_name from information_schema.tables where table_schema='public' order by table_name")) {
        // Only the databasechangelog tables should be in the database after the rollback.
        assertTrue(resultSet.next());
        assertEquals("databasechangelog", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("databasechangeloglock", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  void runLiquibaseCommand(String... commands) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    ImmutableList<String> liquibaseCommand =
        ImmutableList.<String>builder().add("mvn", "-B").add(commands).build();
    builder.command(liquibaseCommand);
    builder.directory(new File(LIQUIBASE_SAMPLE_DIRECTORY));
    Process process = builder.start();

    String errors;
    String output;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    LOGGER.warning(errors);
    LOGGER.info(output);

    int res = process.waitFor();
    assertEquals(errors, 0, res);
  }
}
