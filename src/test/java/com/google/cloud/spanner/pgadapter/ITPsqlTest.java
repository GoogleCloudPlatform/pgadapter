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

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
  private static boolean allAssumptionsPassed = false;

  public static final String POSTGRES_HOST = System.getenv("POSTGRES_HOST");
  public static final String POSTGRES_PORT = System.getenv("POSTGRES_PORT");
  public static final String POSTGRES_USER = System.getenv("POSTGRES_USER");
  public static final String POSTGRES_DATABASE = System.getenv("POSTGRES_DATABASE");
  public static final String POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD");

  private final Random random = new Random();

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
    allAssumptionsPassed = true;

    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    dropDataModel();
    createDataModel();

    testEnv.setUp();
    database = testEnv.createDatabase(DEFAULT_DATA_MODEL);
    testEnv.startPGAdapterServer(Collections.emptyList());
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (allAssumptionsPassed) {
      testEnv.stopPGAdapterServer();
      testEnv.cleanUp();
      dropDataModel();
    }
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

  /**
   * Runs the given SQL statement using psql and returns a String tuple containing the stdout and
   * stderr text.
   */
  private Tuple<String, String> runUsingPsql(String sql) throws IOException, InterruptedException {
    return runUsingPsql(ImmutableList.of(sql));
  }

  private Tuple<String, String> runUsingPsql(Iterable<String> commands)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          "-h",
          (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost()),
          "-p",
          String.valueOf(testEnv.getPGAdapterPort()),
          "-d",
          database.getId().getDatabase()
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
      for (String sql : commands) {
        writer.write(sql);
      }
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }
    process.waitFor();
    return Tuple.of(output, errors);
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
  public void testSelectPgCatalogTables() throws IOException, InterruptedException {
    String sql =
        "select pg_type.typname\n"
            + "from pg_catalog.pg_type\n"
            + "inner join pg_catalog.pg_namespace on pg_type.typnamespace=pg_namespace.oid\n"
            + "order by pg_type.oid;\n";
    Tuple<String, String> result = runUsingPsql(sql);
    String output = result.x(), errors = result.y();

    assertEquals("", errors);
    assertEquals(
        "   typname   \n"
            + "-------------\n"
            + " bool\n"
            + " bytea\n"
            + " int8\n"
            + " int2\n"
            + " int4\n"
            + " text\n"
            + " float4\n"
            + " float8\n"
            + " varchar\n"
            + " date\n"
            + " timestamp\n"
            + " timestamptz\n"
            + " numeric\n"
            + " jsonb\n"
            + "(14 rows)\n",
        output);
  }

  @Test
  public void testPrepareExecuteDeallocate() throws IOException, InterruptedException {
    Tuple<String, String> result =
        runUsingPsql(
            ImmutableList.of(
                "set time zone 'UTC';\n",
                "prepare insert_row as "
                    + "insert into all_types values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);\n",
                "prepare find_row as "
                    + "select * from all_types "
                    + "where col_bigint=$1 "
                    + "and col_bool=$2 "
                    + "and col_bytea=$3 "
                    + "and col_float8=$4 "
                    + "and col_int=$5 "
                    + "and col_numeric=$6 "
                    + "and col_timestamptz=$7 "
                    + "and col_date=$8 "
                    + "and col_varchar=$9 "
                    + "and col_jsonb::text=$10::text;\n",
                "execute find_row (1, true, '\\xaabbcc', 3.14, 100, 6.626, "
                    + "'2022-09-06 17:14:49+02', '2022-09-06', 'hello world', '{\"key\": \"value\"}');\n",
                "execute insert_row (1, true, '\\xaabbcc', 3.14, 100, 6.626, "
                    + "'2022-09-06 17:14:49+02', '2022-09-06', 'hello world', '{\"key\": \"value\"}');\n",
                "execute find_row (1, true, '\\xaabbcc', 3.14, 100, 6.626, "
                    + "'2022-09-06 17:14:49+02', '2022-09-06', 'hello world', '{\"key\": \"value\"}');\n",
                "deallocate find_row;\n",
                "deallocate insert_row;\n"));
    String output = result.x(), errors = result.y();
    assertEquals("", errors);
    assertEquals(
        "SET\n"
            + "PREPARE\n"
            + "PREPARE\n"
            + " col_bigint | col_bool | col_bytea | col_float8 | col_int | col_numeric | col_timestamptz | col_date | col_varchar | col_jsonb \n"
            + "------------+----------+-----------+------------+---------+-------------+-----------------+----------+-------------+-----------\n"
            + "(0 rows)\n"
            + "\n"
            + "INSERT 0 1\n"
            + " col_bigint | col_bool | col_bytea | col_float8 | col_int | col_numeric |    col_timestamptz     |  col_date  | col_varchar |    col_jsonb     \n"
            + "------------+----------+-----------+------------+---------+-------------+------------------------+------------+-------------+------------------\n"
            + "          1 | t        | \\xaabbcc  |       3.14 |     100 |       6.626 | 2022-09-06 15:14:49+00 | 2022-09-06 | hello world | {\"key\": \"value\"}\n"
            + "(1 row)\n"
            + "\n"
            + "DEALLOCATE\n"
            + "DEALLOCATE",
        output);
  }

  /**
   * This test copies data back and forth between PostgreSQL and Cloud Spanner and verifies that the
   * contents are equal after the COPY operation in both directions.
   */
  @Test
  public void testCopyBetweenPostgreSQLAndCloudSpanner() throws Exception {
    int numRows = 100;

    // Generate 99 random rows.
    copyRandomRowsToPostgreSQL(numRows - 1);
    // Also add one row with all nulls to ensure that nulls are also copied correctly.
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into all_types (col_bigint) values (?)")) {
        preparedStatement.setLong(1, random.nextLong());
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

      // COPY the rows from Cloud Spanner to PostgreSQL.
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

  /** This test verifies that we can copy an empty table between PostgreSQL and Cloud Spanner. */
  @Test
  public void testCopyEmptyTableBetweenCloudSpannerAndPostgreSQL() throws Exception {
    // Remove all rows in the table in the local PostgreSQL database.
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      connection.createStatement().executeUpdate("delete from all_types");
    }

    // Execute the COPY tests in both binary and text mode.
    for (boolean binary : new boolean[] {false, true}) {
      // Make sure the all_types table on Cloud Spanner is empty.
      String databaseId = database.getId().getDatabase();
      testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

      // COPY the empty table to Cloud Spanner.
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

      // Verify that still have 0 rows in Cloud Spanner.
      try (Connection connection = DriverManager.getConnection(createJdbcUrlForPGAdapter())) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select count(*) from all_types")) {
          assertTrue(resultSet.next());
          assertEquals(0, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      // COPY the empty table from Cloud Spanner to PostgreSQL.
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
    }
  }

  @Test
  public void testTimestamptzParsing() throws Exception {
    final int numTests = 10;
    for (int i = 0; i < numTests; i++) {
      String timezone;
      try (Connection pgConnection =
          DriverManager.getConnection(
              createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
        int numTimezones;
        try (ResultSet resultSet =
            pgConnection
                .createStatement()
                .executeQuery(
                    "select count(*) from pg_timezone_names where not name like '%posix%' and not name like 'Factory'")) {
          assertTrue(resultSet.next());
          numTimezones = resultSet.getInt(1);
        }

        while (true) {
          try (ResultSet resultSet =
              pgConnection
                  .createStatement()
                  .executeQuery(
                      String.format(
                          "select name from pg_timezone_names where not name like '%%posix%%' and not name like 'Factory' offset %d limit 1",
                          random.nextInt(numTimezones)))) {
            assertTrue(resultSet.next());
            timezone = resultSet.getString(1);
            if (!PROBLEMATIC_ZONE_IDS.contains(ZoneId.of(timezone))) {
              break;
            }
          }
        }
      }
      for (String timestamp :
          new String[] {
            generateRandomLocalDate().toString(),
            generateRandomLocalDateTime().toString().replace('T', ' '),
            generateRandomOffsetDateTime().toString().replace('T', ' ')
          }) {
        ProcessBuilder realPgBuilder = new ProcessBuilder();
        realPgBuilder.command(
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
                + POSTGRES_DATABASE);
        setPgPassword(realPgBuilder);

        ProcessBuilder pgAdapterBuilder = new ProcessBuilder();
        pgAdapterBuilder.command(
            "bash",
            "-c",
            "psql"
                + " -h "
                + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
                + " -p "
                + testEnv.getPGAdapterPort()
                + " -d "
                + database.getId().getDatabase());

        String realPgOutput = "";
        String pgAdapterOutput = "";
        for (ProcessBuilder builder : new ProcessBuilder[] {realPgBuilder, pgAdapterBuilder}) {
          Process process = builder.start();
          try (Writer writer = new OutputStreamWriter(process.getOutputStream())) {
            writer.write(String.format("set time zone '%s';\n", timezone));
            writer.write("prepare test (timestamptz) as select $1;\n");
            writer.write(String.format("execute test ('%s');\n", timestamp));
            writer.write("\\q\n");
          }

          StringBuilder error = new StringBuilder();
          try (Scanner scanner = new Scanner(process.getErrorStream())) {
            while (scanner.hasNextLine()) {
              error.append(scanner.nextLine()).append('\n');
            }
          }
          StringBuilder output = new StringBuilder();
          try (Scanner scanner = new Scanner(process.getInputStream())) {
            while (scanner.hasNextLine()) {
              output.append(scanner.nextLine()).append('\n');
            }
          }
          int res = process.waitFor();
          assertEquals(error.toString(), 0, res);
          if (builder == realPgBuilder) {
            realPgOutput = output.toString();
          } else {
            pgAdapterOutput = output.toString();
          }
        }
        assertEquals(
            String.format("Timestamp: %s, Timezone: %s", timestamp, timezone),
            realPgOutput,
            pgAdapterOutput);
      }
    }
  }

  private static final ImmutableSet<ZoneId> PROBLEMATIC_ZONE_IDS =
      ImmutableSet.of(
          // Mexico abolished DST in 2022, but not all databases contain this information.
          ZoneId.of("America/Chihuahua"),
          // Jordan abolished DST in 2022, but not all databases contain this information.
          ZoneId.of("Asia/Amman"));

  private LocalDate generateRandomLocalDate() {
    return LocalDate.ofEpochDay(random.nextInt(365 * 100));
  }

  private LocalDateTime generateRandomLocalDateTime() {
    return LocalDateTime.of(generateRandomLocalDate(), generateRandomLocalTime());
  }

  private LocalTime generateRandomLocalTime() {
    return LocalTime.ofSecondOfDay(random.nextInt(24 * 60 * 60));
  }

  private OffsetDateTime generateRandomOffsetDateTime() {
    return OffsetDateTime.of(
        generateRandomLocalDateTime(),
        ZoneOffset.ofHoursMinutes(random.nextInt(12), random.nextInt(60)));
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
            + "  (current_date-random()*interval '400 year')::date, md5(random()::text || clock_timestamp()::text)::varchar,"
            + "  ('{\\\"key\\\": \\\"' || md5(random()::text || clock_timestamp()::text)::varchar || '\\\"}')::jsonb "
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
    StringBuilder errors = new StringBuilder();
    try (Scanner scanner = new Scanner(process.getErrorStream())) {
      while (scanner.hasNextLine()) {
        errors.append(scanner.nextLine()).append("\n");
      }
    }
    int res = process.waitFor();
    assertEquals(errors.toString(), 0, res);
  }
}
