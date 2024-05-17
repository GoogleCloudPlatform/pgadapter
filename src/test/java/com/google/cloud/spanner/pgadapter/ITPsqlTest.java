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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement.Format;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.File;
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
import java.time.zone.ZoneRulesException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Logger;
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
  private static final Logger logger = Logger.getLogger(ITPsqlTest.class.getName());

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
    database =
        testEnv.createDatabase(
            ImmutableList.<String>builder()
                .addAll(DEFAULT_DATA_MODEL)
                .add(
                    "create table if not exists singers (\n"
                        + "    id         varchar not null primary key,\n"
                        + "    first_name varchar,\n"
                        + "    last_name  varchar not null,\n"
                        + "    full_name  varchar generated always as (CASE WHEN first_name IS NULL THEN last_name\n"
                        + "                                                 WHEN last_name  IS NULL THEN first_name\n"
                        + "                                                 ELSE first_name || ' ' || last_name END) stored,\n"
                        + "    active     boolean,\n"
                        + "    created_at timestamptz,\n"
                        + "    updated_at timestamptz\n"
                        + ")")
                .add(
                    "create table if not exists albums (\n"
                        + "    id               varchar not null primary key,\n"
                        + "    title            varchar not null,\n"
                        + "    marketing_budget numeric,\n"
                        + "    release_date     date,\n"
                        + "    cover_picture    bytea,\n"
                        + "    singer_id        varchar not null,\n"
                        + "    created_at       timestamptz,\n"
                        + "    updated_at       timestamptz,\n"
                        + "    constraint fk_albums_singers foreign key (singer_id) references singers (id)\n"
                        + ")")
                .add(
                    "create table if not exists tracks (\n"
                        + "    id           varchar not null,\n"
                        + "    track_number bigint not null,\n"
                        + "    title        varchar not null,\n"
                        + "    sample_rate  float8 not null,\n"
                        + "    created_at   timestamptz,\n"
                        + "    updated_at   timestamptz,\n"
                        + "    primary key (id, track_number)\n"
                        + ") interleave in parent albums on delete cascade")
                .add(
                    "create table if not exists track_recordings (\n"
                        + "    id               varchar not null,\n"
                        + "    track_number     bigint not null,\n"
                        + "    recording_number bigint not null,\n"
                        + "    sample_rate      float8 not null,\n"
                        + "    recorded_at      timestamptz not null,\n"
                        + "    primary key (id, track_number, recording_number)\n"
                        + ") interleave in parent tracks on delete cascade\n"
                        + "ttl interval '30 days' on recorded_at")
                .add(
                    "create table if not exists venues (\n"
                        + "    id          varchar not null primary key,\n"
                        + "    name        varchar not null,\n"
                        + "    description varchar not null,\n"
                        + "    created_at  timestamptz,\n"
                        + "    updated_at  timestamptz\n"
                        + ")")
                .add(
                    "create table if not exists concerts (\n"
                        + "    id          varchar not null primary key,\n"
                        + "    venue_id    varchar not null,\n"
                        + "    singer_id   varchar not null,\n"
                        + "    name        varchar not null,\n"
                        + "    start_time  timestamptz not null,\n"
                        + "    end_time    timestamptz not null,\n"
                        + "    created_at  timestamptz,\n"
                        + "    updated_at  timestamptz,\n"
                        + "    constraint fk_concerts_venues  foreign key (venue_id)  references venues  (id),\n"
                        + "    constraint fk_concerts_singers foreign key (singer_id) references singers (id),\n"
                        + "    constraint chk_end_time_after_start_time check (end_time > start_time)\n"
                        + ")")
                .add(
                    "create or replace view v_singers\n"
                        + "sql security invoker\n"
                        + "as\n"
                        + "select id, full_name\n"
                        + "from singers\n"
                        + (System.getenv("SPANNER_EMULATOR_HOST") == null
                            ? "order by last_name, id"
                            : ""))
                .add("create index idx_singers_last_name on singers (last_name)")
                .add(
                    "create unique index idx_tracks_title on tracks (id, title) interleave in albums")
                .add(
                    "create index idx_concerts_start_time on concerts (start_time) include (end_time)")
                .add(
                    "create index idx_singers_albums_release_date on albums (singer_id, release_date desc) where release_date is not null")
                .add(
                    "create change stream cs_singers for singers (first_name, last_name, active)\n"
                        + "with (retention_period = '2d',\n"
                        + "value_capture_type = 'OLD_AND_NEW_VALUES')")
                .addAll(
                    System.getenv("SPANNER_EMULATOR_HOST") == null
                        ? ImmutableList.of(
                            "create role hr_manager", "grant select on table singers to hr_manager")
                        : ImmutableList.of())
                .build());
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
                  .map(s -> s.replace("col_array_int int[]", "col_array_int bigint[]"))
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
          "drop database \"" + POSTGRES_DATABASE + "_dump\" with (force);",
          "-c",
          "/* Dropping schema objects that are created by this test. */\n"
              + "drop view if exists v_singers;\n"
              + "drop table if exists track_recordings;\n"
              + "drop table if exists tracks;\n"
              + "drop table if exists albums;\n"
              + "drop table if exists concerts;\n"
              + "drop table if exists venues;\n"
              + "drop table if exists singers;\n"
              + "drop table if exists all_types;\n"
              + "drop table if exists numbers;\n"
              + "drop foreign table if exists f_all_types;\n"
              + "drop foreign table if exists f_numbers;\n"
              + (System.getenv("SPANNER_EMULATOR_HOST") == null
                  ? "drop role if exists hr_manager;\n"
                  : "")
        };
    ProcessBuilder builder = new ProcessBuilder().command(dropTablesCommand);
    setPgPassword(builder);
    Process process = builder.start();
    String errors = readAll(process.getErrorStream());

    int res = process.waitFor();
    assertEquals(errors, 0, res);
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        result.append(scanner.nextLine()).append("\n");
      }
    }
    return result.toString();
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
        "   typname    \n"
            + "--------------\n"
            + " bool\n"
            + " bytea\n"
            + " int8\n"
            + " int2\n"
            + " int4\n"
            + " text\n"
            + " float4\n"
            + " float8\n"
            + " unknown\n"
            + " _bool\n"
            + " _bytea\n"
            + " _int2\n"
            + " _int4\n"
            + " _text\n"
            + " _varchar\n"
            + " _int8\n"
            + " _float4\n"
            + " _float8\n"
            + " varchar\n"
            + " date\n"
            + " timestamp\n"
            + " _timestamp\n"
            + " _date\n"
            + " timestamptz\n"
            + " _timestamptz\n"
            + " _numeric\n"
            + " numeric\n"
            + " jsonb\n"
            + " _jsonb\n"
            + "(29 rows)\n",
        output);
  }

  @Test
  public void testPrepareExecuteDeallocate() throws IOException, InterruptedException {
    Tuple<String, String> result =
        runUsingPsql(
            ImmutableList.of(
                "set time zone 'UTC';\n",
                "prepare insert_row as "
                    + "insert into all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb)"
                    + " values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);\n",
                "prepare find_row as "
                    + "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb "
                    + "from all_types "
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

  @Test
  public void testSetOperationWithOrderBy() throws IOException, InterruptedException {
    Tuple<String, String> result =
        runUsingPsql(
            "select * from (select 1) one union all select * from (select 2) two order by 1");
    String output = result.x(), errors = result.y();
    assertEquals("", errors);
    assertEquals(" ?column? \n----------\n        1\n        2\n(2 rows)\n", output);
  }

  /**
   * This test copies data back and forth between PostgreSQL and Cloud Spanner and verifies that the
   * contents are equal after the COPY operation in both directions.
   */
  @Test
  public void testCopyBetweenPostgreSQLAndCloudSpanner() throws Exception {
    int numRows = 100;

    truncatePostgreSQLAllTypesTable();
    logger.info("Copying initial data to PG");
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

    logger.info("Verifying initial data in PG");
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

    // Execute the COPY tests in both binary, csv and text mode.
    for (Format format : Format.values()) {
      logger.info("Testing format: " + format);
      // Make sure the all_types table on Cloud Spanner is empty.
      String databaseId = database.getId().getDatabase();
      testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

      logger.info("Copying rows to Spanner");
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
              + " -c \"copy all_types to stdout (format "
              + format
              + ") \" "
              + "  | psql "
              + " -h "
              + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
              + " -p "
              + testEnv.getPGAdapterPort()
              + " -d "
              + database.getId().getDatabase()
              + " -c \"copy all_types from stdin (format "
              + format
              + ")"
              + ";\"\n");
      setPgPassword(builder);
      Process process = builder.start();
      int res = process.waitFor();
      assertEquals(0, res);

      logger.info("Verifying data in Spanner");
      // Verify that we now also have 100 rows in Spanner.
      try (Connection connection = DriverManager.getConnection(createJdbcUrlForPGAdapter())) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select count(*) from all_types")) {
          assertTrue(resultSet.next());
          assertEquals(numRows, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      logger.info("Comparing table contents");
      // Verify that the rows in both databases are equal.
      compareTableContents();

      logger.info("Deleting all data in PG");
      // Remove all rows in the table in the local PostgreSQL database and then copy everything from
      // Cloud Spanner to PostgreSQL.
      try (Connection connection =
          DriverManager.getConnection(
              createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
        assertEquals(numRows, connection.createStatement().executeUpdate("delete from all_types"));
      }

      logger.info("Copying rows to PG");
      // COPY the rows from Cloud Spanner to PostgreSQL.
      ProcessBuilder copyToPostgresBuilder = new ProcessBuilder();
      copyToPostgresBuilder.command(
          "bash",
          "-c",
          "psql"
              + " -c \"copy all_types to stdout (format "
              + format
              + ") \" "
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
              + " -c \"copy all_types from stdin (format "
              + format
              + ")"
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

      logger.info("Compare table contents");
      // Compare table contents again.
      compareTableContents();
    }
  }

  /** This test verifies that we can copy an empty table between PostgreSQL and Cloud Spanner. */
  @Test
  public void testCopyEmptyTableBetweenCloudSpannerAndPostgreSQL() throws Exception {
    logger.info("Deleting all data in PG");
    // Remove all rows in the table in the local PostgreSQL database.
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      connection.createStatement().executeUpdate("delete from all_types");
    }

    // Execute the COPY tests in both binary, csv and text mode.
    for (Format format : Format.values()) {
      logger.info("Testing format: " + format);
      // Make sure the all_types table on Cloud Spanner is empty.
      String databaseId = database.getId().getDatabase();
      testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

      logger.info("Copy empty table to CS");
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
              + " -c \"copy all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "to stdout (format "
              + format
              + ") \" "
              + "  | psql "
              + " -h "
              + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
              + " -p "
              + testEnv.getPGAdapterPort()
              + " -d "
              + database.getId().getDatabase()
              + " -c \"copy all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "from stdin (format "
              + format
              + ")"
              + ";\"\n");
      setPgPassword(builder);
      Process process = builder.start();
      int res = process.waitFor();
      assertEquals(0, res);

      logger.info("Verify that CS is empty");
      // Verify that still have 0 rows in Cloud Spanner.
      try (Connection connection = DriverManager.getConnection(createJdbcUrlForPGAdapter())) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("select count(*) from all_types")) {
          assertTrue(resultSet.next());
          assertEquals(0, resultSet.getLong(1));
          assertFalse(resultSet.next());
        }
      }

      logger.info("Copy empty table to PG");
      // COPY the empty table from Cloud Spanner to PostgreSQL.
      ProcessBuilder copyToPostgresBuilder = new ProcessBuilder();
      copyToPostgresBuilder.command(
          "bash",
          "-c",
          "psql"
              + " -c \"copy all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "to stdout (format "
              + format
              + ") \""
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
              + " -c \"copy all_types (col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
              + "from stdin (format "
              + format
              + ")"
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
            try {
              timezone = resultSet.getString(1);
              if (!PROBLEMATIC_ZONE_IDS.contains(ZoneId.of(timezone))) {
                break;
              }
            } catch (ZoneRulesException ignore) {
              // Skip and try a different one if it is not a supported timezone on this system.
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
          ZoneId.of("Asia/Amman"),
          // Iran observed DST in 1978. Not all databases agree on this.
          ZoneId.of("Asia/Tehran"),
          // Changed the use of DST multiple times. Changed offset on 2024-02-29.
          ZoneId.of("Asia/Almaty"),
          ZoneId.of("Asia/Qostanay"),
          // Rankin_Inlet and Resolute did not observe DST in 1970-1979, but not all databases
          // agree.
          ZoneId.of("America/Rankin_Inlet"),
          ZoneId.of("America/Resolute"),
          ZoneId.of("America/Iqaluit"),
          ZoneId.of("America/Inuvik"),
          ZoneId.of("America/Scoresbysund"),
          // Pangnirtung did not observer DST in 1970-1979, but not all databases agree.
          ZoneId.of("America/Pangnirtung"),
          // Niue switched from -11:30 to -11 in 1978. Not all JDKs know that.
          ZoneId.of("Pacific/Niue"),
          // Ojinaga switched from Mountain to Central time in 2022. Not all JDKs know that.
          ZoneId.of("America/Ojinaga"),
          // Nuuk stopped using DST in 2023. This is unknown to older JDKs.
          ZoneId.of("America/Nuuk"),
          ZoneId.of("America/Godthab"),
          // Antarctica has multiple differences between databases.
          ZoneId.of("Antarctica/Vostok"),
          ZoneId.of("Antarctica/Casey"),
          // Egypt has started using DST again from 2023.
          ZoneId.of("Egypt"));

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
              case Types.ARRAY:
                // Note that we will fall-through to the default if this is not true.
                if (pgResultSet.getObject(col) != null
                    && spangresResultSet.getObject(col) != null) {
                  assertArrayEquals(
                      String.format(
                          "Column %s mismatch:\nPG: %s\nCS: %s",
                          pgResultSet.getMetaData().getColumnName(col),
                          pgResultSet.getArray(col),
                          spangresResultSet.getArray(col)),
                      (Object[]) pgResultSet.getArray(col).getArray(),
                      (Object[]) spangresResultSet.getArray(col).getArray());
                  break;
                }
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

  private void truncatePostgreSQLAllTypesTable() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      connection.createStatement().executeUpdate("truncate table all_types");
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
            + "  random()<0.5, md5(random()::text || clock_timestamp()::text)::bytea, (random()*123456789)::float4, random()*123456789, "
            + "  (random()*999999)::int, (random()*999999999999)::numeric, now()-random()*interval '250 year', "
            + "  (current_date-random()*interval '400 year')::date, md5(random()::text || clock_timestamp()::text)::varchar,"
            + "  ('{\\\"key\\\": \\\"' || md5(random()::text || clock_timestamp()::text)::varchar || '\\\"}')::jsonb, "
            + "  array[(random()*1000000000)::bigint, null, (random()*1000000000)::bigint],\n"
            + "  array[random()<0.5, null, random()<0.5],\n"
            + "  array[md5(random()::text ||clock_timestamp()::text)::bytea, null, md5(random()::text ||clock_timestamp()::text)::bytea],\n"
            + "  array[(random()*123456789)::float4, null, (random()*123456789)::float4],\n"
            + "  array[random()*123456789, null, random()*123456789],\n"
            + "  array[(random()*999999)::int, null, (random()*999999)::int],\n"
            + "  array[(random()*999999)::numeric, null, (random()*999999)::numeric],\n"
            + "  array[now()-random()*interval '50 year', null, now()-random()*interval '50 year'],\n"
            + "  array[(now()-random()*interval '50 year')::date, null, (now()-random()*interval '50 year')::date],\n"
            + "  array[md5(random()::text || clock_timestamp()::text)::varchar, null, md5(random()::text || clock_timestamp()::text)::varchar],\n"
            + "  array[('{\\\"key\\\": \\\"' || md5(random()::text || clock_timestamp()::text)::varchar || '\\\"}')::jsonb, null, ('{\\\"key\\\": \\\"' || md5(random()::text || clock_timestamp()::text)::varchar || '\\\"}')::jsonb]\n"
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
            + " -c \"copy all_types "
            + "from stdin;\"\n");
    setPgPassword(builder);
    Process process = builder.start();
    StringBuilder errors = new StringBuilder();
    try (Scanner scanner = new Scanner(process.getErrorStream())) {
      while (scanner.hasNextLine()) {
        errors.append(scanner.nextLine()).append("\n");
      }
    }
    StringBuilder output = new StringBuilder();
    try (Scanner scanner = new Scanner(process.getInputStream())) {
      while (scanner.hasNextLine()) {
        output.append(scanner.nextLine()).append("\n");
      }
    }
    assertEquals("COPY " + numRows + "\n", output.toString());
    int res = process.waitFor();
    assertEquals(errors.toString(), 0, res);
  }

  @Test
  public void testForeignDataWrapper() throws Exception {
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

    String createForeignTablesCommand =
        DEFAULT_DATA_MODEL.stream()
            .map(s -> s.replace("create table ", "create foreign table f_"))
            .map(s -> s.replace("primary key", ""))
            .map(s -> s.replace("col_int int", "col_int bigint"))
            .map(s -> s.replace("col_array_int int[]", "col_array_int bigint[]"))
            .map(
                s ->
                    s
                        + String.format(
                            " server pgadapter options (table_name '%s');",
                            s.substring(
                                "create foreign table f_".length(),
                                s.indexOf(' ', "create foreign table f_".length()))))
            .collect(Collectors.joining("\n"));

    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write("DROP EXTENSION IF EXISTS postgres_fdw CASCADE;\n");
      writer.write("CREATE EXTENSION IF NOT EXISTS postgres_fdw;\n");
      writer.write(
          String.format(
              "CREATE SERVER IF NOT EXISTS pgadapter FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%d', dbname '%s', options '-c spanner.read_only=true');\n",
              (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost()),
              testEnv.getPGAdapterPort(),
              database.getId().getDatabase()));
      writer.write(
          String.format(
              "CREATE USER MAPPING IF NOT EXISTS FOR %s SERVER pgadapter OPTIONS (user '%s', password_required 'false');\n",
              POSTGRES_USER, POSTGRES_USER));
      writer.write(
          String.format("GRANT USAGE ON FOREIGN SERVER pgadapter TO %s;\n", POSTGRES_USER));
      writer.write(createForeignTablesCommand);
      writer.write("truncate table all_types;\n");
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertTrue(errors, "".equals(errors) || !errors.contains("ERROR:"));
    assertEquals(
        "DROP EXTENSION\n"
            + "CREATE EXTENSION\n"
            + "CREATE SERVER\n"
            + "CREATE USER MAPPING\n"
            + "GRANT\n"
            + "CREATE FOREIGN TABLE\n"
            + "CREATE FOREIGN TABLE\n"
            + "TRUNCATE TABLE",
        output);
    int res = process.waitFor();
    assertEquals(0, res);

    truncatePostgreSQLAllTypesTable();
    // Generate 250 random rows.
    int numRows = 250;
    copyRandomRowsToPostgreSQL(numRows - 1);
    // Also add one row with all nulls to ensure that nulls are also copied correctly.
    long nullRowId = random.nextLong();
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into all_types (col_bigint) values (?)")) {
        preparedStatement.setLong(1, nullRowId);
        assertEquals(1, preparedStatement.executeUpdate());
      }
    }

    // Make sure the all_types table on Cloud Spanner is empty.
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));
    // COPY the rows to Cloud Spanner.
    builder = new ProcessBuilder();
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
            + " -c \"copy all_types to stdout (format binary) \" "
            + "  | psql "
            + " -h "
            + (POSTGRES_HOST.startsWith("/") ? "/tmp" : testEnv.getPGAdapterHost())
            + " -p "
            + testEnv.getPGAdapterPort()
            + " -d "
            + database.getId().getDatabase()
            + " -c \"copy all_types from stdin (format binary)"
            + ";\"\n");
    setPgPassword(builder);
    process = builder.start();
    res = process.waitFor();
    assertEquals(0, res);

    // Make sure we can access the data in Cloud Spanner through the foreign data wrapper.
    int foundRows = 0;
    int foundNullRows = 0;
    try (Connection connection =
        DriverManager.getConnection(createJdbcUrlForLocalPg(), POSTGRES_USER, POSTGRES_PASSWORD)) {
      connection
          .createStatement()
          .execute("set session characteristics as transaction isolation level serializable");
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from f_all_types")) {
        while (resultSet.next()) {
          foundRows++;
          if (resultSet.getLong(1) == nullRowId) {
            foundNullRows++;
          }
          for (int col = 2; col <= resultSet.getMetaData().getColumnCount(); col++) {
            if (resultSet.getLong(1) == nullRowId) {
              assertNull(resultSet.getObject(col));
            } else {
              assertNotNull(resultSet.getObject(col));
            }
          }
        }
      }
      assertEquals(numRows, foundRows);
      assertEquals(1, foundNullRows);

      // Also verify that we can use parameterized queries with the foreign table.
      // These parameters are however converted to literals by the PostgreSQL Foreign Data Wrapper.
      // That means that the queries that are executed on Cloud Spanner are always unparameterized.
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("select * from f_all_types where col_bigint=?")) {
        preparedStatement.setLong(1, nullRowId);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertTrue(resultSet.next());
          for (int col = 2; col <= resultSet.getMetaData().getColumnCount(); col++) {
            assertNull(resultSet.getObject(col));
          }
          assertFalse(resultSet.next());
        }
      }
    }

    // Create a user-defined function and use it on the data in Cloud Spanner.
    builder = new ProcessBuilder();
    builder.command(psqlCommand);
    setPgPassword(builder);
    process = builder.start();
    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write("set session characteristics as transaction isolation level serializable;");
      writer.write("DROP FUNCTION IF EXISTS date_timestamp_year_diff;\n");
      writer.write(
          "CREATE FUNCTION date_timestamp_year_diff(ts timestamptz, dt date) RETURNS integer AS $$\n"
              + "    SELECT extract(year from ts) - extract(year from dt);\n"
              + "$$ LANGUAGE SQL;\n");
      writer.write(
          "select date_timestamp_year_diff(col_timestamptz, col_date) as year_diff\n"
              + "from f_all_types\n"
              + "order by year_diff asc nulls first;\n");
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }
    assertTrue(errors, "".equals(errors) || !errors.contains("ERROR:"));
    assertTrue(
        output,
        output.startsWith(
            "SET\n"
                + "DROP FUNCTION\n"
                + "CREATE FUNCTION\n"
                + " year_diff \n"
                + "-----------\n"
                + "          \n"));
    assertTrue(output, output.endsWith("(250 rows)\n"));
    res = process.waitFor();
    assertEquals(0, res);
  }

  @Test
  public void testPgDump_PgRestore() throws Exception {
    createDumpDatabase();

    ProcessBuilder builder = new ProcessBuilder();
    String[] command = new String[] {"./run-pg-dump.sh"};

    builder.directory(new File("./local-postgresql-copy"));
    builder.environment().put("PGADAPTER_HOST", "localhost");
    builder.environment().put("PGADAPTER_PORT", String.valueOf(testEnv.getPGAdapterPort()));
    builder.environment().put("SPANNER_DATABASE", database.getId().getDatabase());

    builder.environment().put("POSTGRES_HOST", POSTGRES_HOST);
    builder.environment().put("POSTGRES_PORT", POSTGRES_PORT);
    builder.environment().put("POSTGRES_USER", POSTGRES_USER);
    builder.environment().put("POSTGRES_DB", POSTGRES_DATABASE + "_dump");
    if (POSTGRES_PASSWORD != null) {
      builder.environment().put("POSTGRES_PASSWORD", POSTGRES_PASSWORD);
    }
    builder.command(command);
    Process process = builder.start();
    String errors;
    String output;
    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write("Y\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }
    assertEquals(errors, 0, process.waitFor());
    assertTrue(output, output.contains("Finished running pg_dump"));
  }

  private void createDumpDatabase() throws Exception {
    String[] createDatabaseCommand =
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
    ProcessBuilder builder = new ProcessBuilder().command(createDatabaseCommand);
    setPgPassword(builder);
    Process process = builder.start();

    String errors;
    try (OutputStreamWriter writer = new OutputStreamWriter(process.getOutputStream());
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      writer.write(
          "SELECT 'CREATE DATABASE \""
              + POSTGRES_DATABASE
              + "_dump\"'\n"
              + "WHERE NOT EXISTS ("
              + "SELECT "
              + "FROM pg_database "
              + "WHERE datname = '"
              + POSTGRES_DATABASE
              + "_dump') \\gexec\n");
      writer.write("\\q\n");
      writer.flush();
      errors = errorReader.lines().collect(Collectors.joining("\n"));
    }
    int res = process.waitFor();
    assertEquals(errors, 0, res);
  }
}
