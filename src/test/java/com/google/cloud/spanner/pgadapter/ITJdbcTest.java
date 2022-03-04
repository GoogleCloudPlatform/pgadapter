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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class ITJdbcTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static ProxyServer server;
  private static Database database;

  @Parameter public String preferredQueryMode;

  @Parameters(name = "preferredQueryMode = {0}")
  public static Object[] data() {
    return new Object[] {"extended", "simple"};
  }

  @BeforeClass
  public static void setup() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    // TODO: Refactor the integration tests to use a common subclass, as this is repeated in each
    // class.
    testEnv.setUp();
    if (testEnv.isUseExistingDb()) {
      database = testEnv.getExistingDatabase();
    } else {
      database = testEnv.createDatabase();
      testEnv.updateDdl(
          database.getId().getDatabase(),
          Arrays.asList(
              "create table numbers (num int not null primary key, name varchar(100))",
              "create table all_types ("
                  + "col_bigint bigint not null primary key, "
                  + "col_bool bool, "
                  + "col_bytea bytea, "
                  + "col_float8 float8, "
                  + "col_int int, "
                  + "col_numeric numeric, "
                  + "col_timestamptz timestamptz, "
                  + "col_varchar varchar(100))"));
    }
    String credentials = testEnv.getCredentials();
    ImmutableList.Builder<String> argsListBuilder =
        ImmutableList.<String>builder()
            .add(
                "-p",
                testEnv.getProjectId(),
                "-i",
                testEnv.getInstanceId(),
                "-d",
                database.getId().getDatabase(),
                "-s",
                String.valueOf(testEnv.getPort()),
                "-e",
                testEnv.getUrl().getHost());
    if (credentials != null) {
      argsListBuilder.add("-c", testEnv.getCredentials());
    }
    String[] args = argsListBuilder.build().toArray(new String[0]);
    server = new ProxyServer(new OptionsMetadata(args));
    server.startServer();
  }

  @AfterClass
  public static void teardown() {
    if (server != null) {
      server.stopServer();
    }
    testEnv.cleanUp();
  }

  @Before
  public void insertTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("numbers", KeySet.all())));
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));
    testEnv.write(
        databaseId,
        Arrays.asList(
            Mutation.newInsertBuilder("numbers").set("num").to(1L).set("name").to("One").build(),
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(1)
                .set("col_numeric")
                .to(new BigDecimal("3.14"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00"))
                .set("col_varchar")
                .to("test")
                .build()));
  }

  private String getConnectionUrl() {
    return String.format(
        "jdbc:postgresql://localhost:%d/?preferredQueryMode=%s",
        server.getLocalPort(), preferredQueryMode);
  }

  @Test
  public void testSelectHelloWorld() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("SELECT 'Hello World!'")) {
        assertTrue(resultSet.next());
        assertEquals("Hello World!", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testSelectWithParameters() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_varchar "
                  + "from all_types "
                  + "where col_bigint=? "
                  + "and col_bool=? "
                  + "and col_bytea=? "
                  + "and col_float8=? "
                  + "and col_int=? "
                  + "and col_numeric=? "
                  + "and col_timestamptz=? "
                  + "and col_varchar=?")) {

        int index = 0;
        statement.setLong(++index, 1);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d);
        statement.setInt(++index, 1);
        statement.setBigDecimal(++index, new BigDecimal("3.14"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp());
        statement.setString(++index, "test");

        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());

          assertEquals(1, resultSet.getLong(1));
          assertTrue(resultSet.getBoolean(2));
          assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
          assertEquals(3.14d, resultSet.getDouble(4), 0.0d);
          assertEquals(1, resultSet.getInt(5));
          assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(6));
          assertEquals(
              Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp(),
              resultSet.getTimestamp(7));
          assertEquals("test", resultSet.getString(8));

          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testInsertWithParameters() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setBoolean(++index, true);
        statement.setBytes(++index, "bytes_test".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 10.1);
        statement.setInt(++index, 100);
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp());
        statement.setString(++index, "string_test");

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=2")) {
        assertTrue(resultSet.next());

        assertEquals(2, resultSet.getLong(1));
        assertTrue(resultSet.getBoolean(2));
        assertArrayEquals("bytes_test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
        assertEquals(10.1d, resultSet.getDouble(4), 0.0d);
        assertEquals(100, resultSet.getInt(5));
        assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(6));
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(7));
        assertEquals("string_test", resultSet.getString(8));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testUpdateWithParameters() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "update all_types set "
                  + "col_bool=?, "
                  + "col_bytea=?, "
                  + "col_float8=?, "
                  + "col_int=?, "
                  + "col_numeric=?, "
                  + "col_timestamptz=?, "
                  + "col_varchar=? "
                  + "where col_bigint=?")) {
        int index = 0;
        statement.setBoolean(++index, false);
        statement.setBytes(++index, "updated".getBytes(StandardCharsets.UTF_8));
        statement.setDouble(++index, 3.14d * 2d);
        statement.setInt(++index, 2);
        statement.setBigDecimal(++index, new BigDecimal("10.0"));
        // Note that PostgreSQL does not support nanosecond precision, so the JDBC driver therefore
        // truncates this value before it is sent to PG.
        statement.setTimestamp(
            ++index,
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123456789+01:00").toSqlTimestamp());
        statement.setString(++index, "updated");
        statement.setLong(++index, 1);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=1")) {
        assertTrue(resultSet.next());

        assertEquals(1, resultSet.getLong(1));
        assertFalse(resultSet.getBoolean(2));
        assertArrayEquals("updated".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
        assertEquals(3.14d * 2d, resultSet.getDouble(4), 0.0d);
        assertEquals(2, resultSet.getInt(5));
        assertEquals(new BigDecimal("10.0"), resultSet.getBigDecimal(6));
        // Note: The JDBC driver already truncated the timestamp value before it was sent to PG.
        // So here we read back the truncated value.
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123457+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(7));
        assertEquals("updated", resultSet.getString(8));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNullValues() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // TODO: Add col_timestamptz to the statement when PgAdapter allows untyped null values.
      // The PG JDBC driver sends date and timestamp parameters with type code Oid.UNSPECIFIED.
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        // TODO: Enable the next line when PgAdapter allows untyped null values.
        // This is currently blocked on both the Spangres backend allowing untyped null values in
        // SQL statements, as well as the Java client library and/or JDBC driver allowing untyped
        // null values.
        // See also https://github.com/googleapis/java-spanner/pull/1680
        // statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.VARCHAR);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=2")) {
        assertTrue(resultSet.next());

        int index = 0;
        assertEquals(2, resultSet.getLong(++index));

        // Note: JDBC returns the zero-value for primitive types if the value is NULL, and you have
        // to call wasNull() to determine whether the value was NULL or zero.
        assertFalse(resultSet.getBoolean(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBytes(++index));
        assertTrue(resultSet.wasNull());
        assertEquals(0d, resultSet.getDouble(++index), 0.0d);
        assertTrue(resultSet.wasNull());
        assertEquals(0, resultSet.getInt(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getBigDecimal(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getTimestamp(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(++index));
        assertTrue(resultSet.wasNull());

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertWithLiterals() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      int updateCount =
          connection
              .createStatement()
              .executeUpdate("insert into numbers (num, name) values (2, 'Two')");
      assertEquals(1, updateCount);
    }
  }

  @Test
  public void testUpdateWithLiterals() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      int updateCount =
          connection
              .createStatement()
              .executeUpdate("update numbers set name='One - updated' where num=1");
      assertEquals(1, updateCount);

      // This should return a zero update count, as there is no row 2.
      int noUpdateCount =
          connection
              .createStatement()
              .executeUpdate("update numbers set name='Two - updated' where num=2");
      assertEquals(0, noUpdateCount);
    }
  }

  /**
   * ---------------------------------------------------------------------------------------------/*
   * COPY tests
   *
   * <p>The data that is used for the COPY tests were generated using this statement:
   *
   * <p><code>
   * select (random()*1000000000)::bigint, random()<0.5, md5(random()::text ||
   * clock_timestamp()::text)::bytea, random()*123456789, (random()*999999)::int,
   * (random()*999999)::numeric, now()-random()*interval '50 year', md5(random()::text ||
   * clock_timestamp()::text)::varchar from generate_series(1, 1000000) s(i);
   * </code> Example for streaming large amounts of random data from PostgreSQL to Cloud Spanner.
   * This must be run with the system property -Dcopy_in_insert_or_update=true set, or otherwise it
   * will eventually fail on a unique key constraint violation, as the primary key value is a random
   * number. <code>
   *   psql -h localhost -d knut-test-db -c "copy (select (random()*1000000000)::bigint, random()<0.5, md5(random()::text || clock_timestamp()::text)::bytea, random()*123456789, (random()*999999)::int, (random()*999999)::numeric, now()-random()*interval '50 year', md5(random()::text || clock_timestamp()::text)::varchar from generate_series(1, 1000000) s(i)) to stdout" | psql -h localhost -p 5433 -d test -c "set autocommit_dml_mode='partitioned_non_atomic'" -c "copy all_types from stdin;"
   * </code>
   */
  @Test
  public void testCopyIn_Small() throws SQLException, IOException {
    // Empty all data in the table.
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types from stdin;",
              new FileInputStream("./src/test/resources/all_types_data_small.txt"));
      assertEquals(100L, copyCount);

      // Verify that there are actually 100 rows in the table.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from all_types")) {
        assertTrue(resultSet.next());
        assertEquals(100L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testCopyIn_Large_FailsWhenAtomic() throws SQLException {
    // Empty all data in the table.
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  copyManager.copyIn(
                      "copy all_types from stdin;",
                      new FileInputStream("./src/test/resources/all_types_data.txt")));
      assertTrue(exception.getMessage().contains("Record count"));
      assertTrue(exception.getMessage().contains("has exceeded the limit"));

      // Verify that the table is still empty.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from all_types")) {
        assertTrue(resultSet.next());
        assertEquals(0L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testCopyIn_Large_SucceedsWhenNonAtomic() throws SQLException, IOException {
    // Empty all data in the table.
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      connection.createStatement().execute("set autocommit_dml_mode='partitioned_non_atomic'");

      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types from stdin;",
              new FileInputStream("./src/test/resources/all_types_data.txt"));
      assertEquals(10_000L, copyCount);

      // Verify that the table is still empty.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from all_types")) {
        assertTrue(resultSet.next());
        assertEquals(10_000L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Delete the imported data to prevent the cleanup method to fail on 'Too many mutations' when
      // it tries to delete all data using a normal transaction.
      connection.createStatement().execute("delete from all_types");
    }
  }
}
