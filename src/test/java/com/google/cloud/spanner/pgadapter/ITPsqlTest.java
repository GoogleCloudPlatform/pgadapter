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

package com.google.cloud.spanner.pgadapter;

import static com.google.cloud.spanner.pgadapter.PgAdapterTestEnv.DEFAULT_DATA_MODEL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This integration test assumes that a local PostgreSQL server is available, and uses that server
 * to compare the behavior of PGAdapter with a real PostgreSQL server. It for example generates
 * random data that is inserted into the local PostgreSQL database, copies this to Cloud Spanner,
 * and then compares the data in the two databases afterwards.
 */
@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPsqlTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  public static final String POSTGRES_HOST = System.getenv("POSTGRES_HOST");
  public static final String POSTGRES_PORT = System.getenv("POSTGRES_PORT");
  public static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
  public static final String POSTGRES_DATABASE = System.getenv("POSTGRES_DATABASE");
  public static final String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");

  @BeforeClass
  public static void setup() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    assumeTrue("This test requires bash to be installed", isBashAvailable());
    assumeFalse(
        "This test the environment variable POSTGRES_HOST to point to a valid PostgreSQL host",
        Strings.isNullOrEmpty(POSTGRES_HOST));
    assumeFalse(
        "This test the environment variable POSTGRES_PORT to point to a valid PostgreSQL port number",
        Strings.isNullOrEmpty(POSTGRES_PORT));
    assumeFalse(
        "This test the environment variable POSTGRES_USER to point to a valid PostgreSQL user",
        Strings.isNullOrEmpty(POSTGRES_USER));
    assumeFalse(
        "This test the environment variable POSTGRES_DATABASE to point to a valid PostgreSQL database",
        Strings.isNullOrEmpty(POSTGRES_DATABASE));

    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    dropDataModel();
    createDataModel();

    testEnv.setUp();
    database = testEnv.createDatabase(DEFAULT_DATA_MODEL);
    testEnv.startPGAdapterServer(ImmutableList.of("-disable_psql_hints"));
  }

  @AfterClass
  public static void teardown() throws Exception {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
    dropDataModel();
  }

  private static boolean isPsqlAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static boolean isBashAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"bash", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static ProcessBuilder setPgPassword(ProcessBuilder builder) {
    if (POSTGRES_PASSWORD != null) {
      builder.environment().put("PGPASSWORD", POSTGRES_PASSWORD);
    }
    return builder;
  }

  private static void createDataModel() throws Exception {
    String[] createTablesCommand =
        new String[] {
          "psql",
          "-h",
          POSTGRES_HOST,
          "-p",
          POSTGRES_PORT,
          "-U",
          POSTGRES_USER,
          "-d",
          POSTGRES_DATABASE,
          "-c",
          // We need to change the `col_int int` column definition into `col_int bigint` to make the
          // local PostgreSQL database match the actual data model of the Cloud Spanner database.
          DEFAULT_DATA_MODEL.stream()
                  .map(s -> s.replace("col_int int", "col_int bigint"))
                  .collect(Collectors.joining(";"))
              + ";\n"
        };
    ProcessBuilder builder = new ProcessBuilder().command(createTablesCommand);
    setPgPassword(builder);
    Process process = builder.start();

    int res = process.waitFor();
    assertEquals(0, res);
  }

  private static void dropDataModel() throws Exception {
    String[] dropTablesCommand =
        new String[] {
          "psql",
          "-h",
          POSTGRES_HOST,
          "-p",
          POSTGRES_PORT,
          "-U",
          POSTGRES_USER,
          "-d",
          POSTGRES_DATABASE,
          "-c",
          "drop table if exists all_types; drop table if exists numbers;"
        };
    ProcessBuilder builder = new ProcessBuilder().command(dropTablesCommand);
    setPgPassword(builder);
    Process process = builder.start();

    int res = process.waitFor();
    assertEquals(0, res);
  }

  private String createJdbcUrlForLocalPg() {
    if (POSTGRES_HOST.startsWith("/")) {
      return String.format(
          "jdbc:postgresql://localhost/%s?"
              + "socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=%s/.s.PGSQL.%s",
          POSTGRES_DATABASE, POSTGRES_HOST, POSTGRES_PORT);
    }
    return String.format(
        "jdbc:postgresql://%s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE);
  }

  private String createJdbcUrlForPGAdapter() {
    if (POSTGRES_HOST.startsWith("/")) {
      return String.format(
          "jdbc:postgresql://localhost/%s?"
              + "&socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
          database.getId().getDatabase(), testEnv.getPGAdapterPort());
    }
    return String.format(
        "jdbc:postgresql://%s/%s",
        testEnv.getPGAdapterHostAndPort(), database.getId().getDatabase());
  }

  @Test
  public void testRealPostgreSQLHelloWorld() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          "-h",
          POSTGRES_HOST,
          "-p",
          POSTGRES_PORT,
          "-U",
          POSTGRES_USER,
          "-d",
          POSTGRES_DATABASE
        };
    builder.command(psqlCommand);
    setPgPassword(builder);
    Process process = builder.start();
    String errors;
    String output;

    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write("SELECT 'Hello World!' AS hello;\n");
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals("    hello     \n" + "--------------\n" + " Hello World!\n" + "(1 row)\n", output);
    int res = process.waitFor();
    assertEquals(0, res);
  }

  @Test
  public void testCopyFromPostgreSQLToCloudSpanner() throws Exception {
    int numRows = 100;

    // Generate 99 random rows.
    copyRandomRowsToPostgreSQL(numRows - 1);
    // Also add one row with all nulls to ensure that nulls are also copied correctly.
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into all_types (col_bigint) values (?)")) {
        preparedStatement.setLong(1, new Random().nextLong());
        assertEquals(1, preparedStatement.executeUpdate());
      }
    }

    // Verify that we have 100 rows in PostgreSQL.
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from all_types")) {
        assertTrue(resultSet.next());
        assertEquals(numRows, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }

    // Execute the COPY tests in both binary and text mode.
    for (boolean binary : new boolean[] {false, true}) {
      // Make sure the all_types table on Cloud Spanner is empty.
      String databaseId = database.getId().getDatabase();
      testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

      // COPY the rows to Cloud Spanner.
      ProcessBuilder builder = new ProcessBuilder();
      builder.command(
          "bash",
          "-c",
          "psql"
              + " -h "
              + POSTGRES_HOST
              + " -p "
              + POSTGRES_PORT
              + " -U "
              + POSTGRES_USER
              + " -d "
              + POSTGRES_DATABASE
              + " -c \"copy all_types to stdout"
              + (binary ? " binary \" " : "\" ")
              + "  | psql "
              + " -h "
              + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
              + " -p "
              + testEnv.getPGAdapterPort()
              + " -d "
              + database.getId().getDatabase()
              + " -c \"copy all_types from stdin "
              + (binary ? "binary" : "")
              + ";\"\n");
      setPgPassword(builder);
      Process process = builder.start();
      int res = process.waitFor();
      assertEquals(0, res);

      // Verify that we now also have 100 rows in Spanner.
      try (Connection connection = DriverManager.getConnection(createJdbcUrlForPGAdapter())) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select count(*) from all_types")) {
          assertTrue(resultSet.next());
          assertEquals(numRows, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      // Verify that the rows in both databases are equal.
      compareTableContents();

      // Remove all rows in the table in the local PostgreSQL database and then copy everything from
      // Cloud Spanner to PostgreSQL.
      try (Connection connection =
          DriverManager.getConnection(
              createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
        assertEquals(numRows, connection.createStatement().executeUpdate("delete from all_types"));
      }

      // COPY the rows to Cloud Spanner.
      ProcessBuilder copyToPostgresBuilder = new ProcessBuilder();
      copyToPostgresBuilder.command(
          "bash",
          "-c",
          "psql"
              + " -c \"copy all_types to stdout"
              + (binary ? " binary \" " : "\" ")
              + " -h "
              + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
              + " -p "
              + testEnv.getPGAdapterPort()
              + " -d "
              + database.getId().getDatabase()
              + "  | psql "
              + " -h "
              + POSTGRES_HOST
              + " -p "
              + POSTGRES_PORT
              + " -U "
              + POSTGRES_USER
              + " -d "
              + POSTGRES_DATABASE
              + " -c \"copy all_types from stdin "
              + (binary ? "binary" : "")
              + ";\"\n");
      setPgPassword(copyToPostgresBuilder);
      Process copyToPostgresProcess = copyToPostgresBuilder.start();
      InputStream errorStream = copyToPostgresProcess.getErrorStream();
      int copyToPostgresResult = copyToPostgresProcess.waitFor();
      StringBuilder errors = new StringBuilder();
      try (Scanner scanner = new Scanner(new InputStreamReader(errorStream))) {
        while (scanner.hasNextLine()) {
          errors.append(errors).append(scanner.nextLine()).append("\n");
        }
      }
      assertEquals("", errors.toString());
      assertEquals(0, copyToPostgresResult);

      // Compare table contents again.
      compareTableContents();
    }
  }

  private void compareTableContents() throws SQLException {
    try (Connection pgConnection =
            DriverManager.getConnection(
                createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD);
        Connection spangresConnection = DriverManager.getConnection(createJdbcUrlForPGAdapter())) {
      try (ResultSet pgResultSet =
              pgConnection
                  .createStatement()
                  .executeQuery("select * from all_types order by col_bigint");
          ResultSet spangresResultSet =
              spangresConnection
                  .createStatement()
                  .executeQuery("select * from all_types order by col_bigint")) {
        assertEquals(
            pgResultSet.getMetaData().getColumnCount(),
            spangresResultSet.getMetaData().getColumnCount());
        while (pgResultSet.next() && spangresResultSet.next()) {
          for (int col = 1; col <= pgResultSet.getMetaData().getColumnCount(); col++) {
            switch (pgResultSet.getMetaData().getColumnType(col)) {
              case Types.BINARY:
                // We can't use equals on byte arrays.
                assertArrayEquals(
                    String.format(
                        "Column %s mismatch:\nPG: %s\nCS: %s",
                        pgResultSet.getMetaData().getColumnName(col),
                        Arrays.toString(pgResultSet.getBytes(col)),
                        Arrays.toString(spangresResultSet.getBytes(col))),
                    pgResultSet.getBytes(col),
                    spangresResultSet.getBytes(col));
                break;
              case Types.INTEGER:
                // Spangres accepts `int4` as a data type in DDL statements, but it is converted to
                // `int8` by the backend. This means that there is a slight difference between these
                // two in the local database and Cloud Spanner.
                assertEquals(
                    String.format(
                        "Column %s mismatch:\nPG: %s\nCS: %s",
                        pgResultSet.getMetaData().getColumnName(col),
                        pgResultSet.getObject(col),
                        spangresResultSet.getObject(col)),
                    pgResultSet.getLong(col),
                    spangresResultSet.getLong(col));
                assertEquals(pgResultSet.wasNull(), spangresResultSet.wasNull());
                break;
              default:
                assertEquals(
                    String.format(
                        "Column %s mismatch:\nPG: %s\nCS: %s",
                        pgResultSet.getMetaData().getColumnName(col),
                        pgResultSet.getObject(col),
                        spangresResultSet.getObject(col)),
                    pgResultSet.getObject(col),
                    spangresResultSet.getObject(col));
            }
          }
        }
      }
    }
  }

  private void copyRandomRowsToPostgreSQL(int numRows) throws Exception {
    // Generate random rows and copy these into the all_types table in a local PostgreSQL database.
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "bash",
        "-c",
        "psql"
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy (select (random()*9223372036854775807)::bigint, "
            + "  random()<0.5, md5(random()::text || clock_timestamp()::text)::bytea, random()*123456789, "
            + "  (random()*999999)::int, (random()*999999999999)::numeric, now()-random()*interval '250 year', "
            + "  (current_date-random()*interval '400 year')::date, md5(random()::text || clock_timestamp()::text)::varchar "
            + String.format("  from generate_series(1, %d) s(i)) to stdout\" ", numRows)
            + "  | psql "
            + " -h "
            + POSTGRES_HOST
            + " -p "
            + POSTGRES_PORT
            + " -U "
            + POSTGRES_USER
            + " -d "
            + POSTGRES_DATABASE
            + " -c \"copy all_types from stdin;\"\n");
    setPgPassword(builder);
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);
  }
}
