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
  private static Database database;

  @Parameter public String preferQueryMode;

  @Parameters(name = "preferQueryMode = {0}")
  public static Object[] data() {
    return new Object[] {"extended"};
  }

  @BeforeClass
  public static void setup() throws ClassNotFoundException {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");

    testEnv.setUp();
    database = testEnv.createDatabase(PgAdapterTestEnv.DEFAULT_DATA_MODEL);
    testEnv.startPGAdapterServer(Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
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
        "jdbc:postgresql://%s/%s?preferQueryMode=%s",
        testEnv.getPGAdapterHostAndPort(), database.getId().getDatabase(), preferQueryMode);
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
    boolean isSimpleMode = "simple".equalsIgnoreCase(preferQueryMode);
    String sql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_varchar "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            // The PG JDBC driver does not support bytea parameters in simple mode.
            + (isSimpleMode ? "" : "and col_bytea=? ")
            + "and col_float8=? "
            + "and col_int=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_varchar=?";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {

        int index = 0;
        statement.setLong(++index, 1);
        statement.setBoolean(++index, true);
        if (!isSimpleMode) {
          statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        }
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
          if (!isSimpleMode) {
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
          }
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
    boolean isSimpleMode = "simple".equalsIgnoreCase(preferQueryMode);
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_varchar) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setBoolean(++index, true);
        // The PG JDBC driver does not support bytea parameters in simple mode.
        if (isSimpleMode) {
          statement.setNull(++index, Types.BINARY);
        } else {
          statement.setBytes(++index, "bytes_test".getBytes(StandardCharsets.UTF_8));
        }
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
        if (!isSimpleMode) {
          assertArrayEquals("bytes_test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
        }
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
    boolean isSimpleMode = "simple".equalsIgnoreCase(preferQueryMode);
    String sql =
        "update all_types set "
            + "col_bool=?, "
            // The PG JDBC driver does not support bytea parameters in simple mode.
            + (isSimpleMode ? "" : "col_bytea=?, ")
            + "col_float8=?, "
            + "col_int=?, "
            + "col_numeric=?, "
            + "col_timestamptz=?, "
            + "col_varchar=? "
            + "where col_bigint=?";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        int index = 0;
        statement.setBoolean(++index, false);
        if (!isSimpleMode) {
          statement.setBytes(++index, "updated".getBytes(StandardCharsets.UTF_8));
        }
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
        if (!isSimpleMode) {
          assertArrayEquals("updated".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(3));
        }
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
   *   psql -h localhost -d knut-test-db -c "copy (select (random()*1000000000)::bigint, random()<0.5, md5(random()::text || clock_timestamp()::text)::bytea, random()*123456789, (random()*999999)::int, (random()*999999)::numeric, now()-random()*interval '50 year', md5(random()::text || clock_timestamp()::text)::varchar from generate_series(1, 1000000) s(i)) to stdout" | psql -h localhost -p 5433 -d test -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'" -c "copy all_types from stdin;"
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
      // The JDBC driver CopyManager takes the COPY protocol quite literally, and as the COPY
      // protocol does not include any error handling, the JDBC driver will just send all data to
      // the server and ignore any error messages the server might send during the copy operation.
      // PGAdapter therefore drops the connection if it continues to receive CopyData messages after
      // it sent back an error message.
      assertTrue(exception.getMessage().contains("Database connection failed"));
    }

    // Verify that the table is still empty.
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
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
      connection
          .createStatement()
          .execute("set spanner.autocommit_dml_mode='partitioned_non_atomic'");

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

      // Delete the imported data to prevent the cleanup method to fail on 'Too many mutations'
      // when
      // it tries to delete all data using a normal transaction.
      connection.createStatement().execute("delete from all_types");
    }
  }

  @Test
  public void testTwoDmlStatementsInSimpleMode() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        // Statement#execute(String) returns false if the result is an update count or no result.
        assertFalse(
            statement.execute(
                "INSERT INTO numbers VALUES (2, 'Two'); UPDATE numbers SET name=name || ' - Updated';"));

        // Note that we have sent two DML statements to the database in one string. These should be
        // treated as separate statements, and there should therefore be two results coming back
        // from the server. That is; The first update count should be 1 (the INSERT), and the second
        // should be 2 (the UPDATE).
        assertEquals(1, statement.getUpdateCount());

        // The following is a prime example of how not to design an API, but this is how JDBC works.
        // getMoreResults() returns true if the next result is a ResultSet. However, if the next
        // result is an update count, it returns false, and we have to check getUpdateCount() to
        // verify whether there were any more results.
        assertFalse(statement.getMoreResults());
        assertEquals(2, statement.getUpdateCount());

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());

        // Read back the data to verify.
        try (ResultSet resultSet =
            statement.executeQuery("SELECT name FROM numbers ORDER BY num")) {
          assertTrue(resultSet.next());
          assertEquals("One - Updated", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Two - Updated", resultSet.getString("name"));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testDmlAndQueryInBatchInSimpleMode() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (java.sql.Statement statement = connection.createStatement()) {
        assertFalse(
            statement.execute(
                "INSERT INTO numbers VALUES (2, 'Two'); SELECT name FROM numbers ORDER BY num;"));
        assertEquals(1, statement.getUpdateCount());

        assertTrue(statement.getMoreResults());
        try (ResultSet resultSet = statement.getResultSet()) {
          assertTrue(resultSet.next());
          assertEquals("One", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Two", resultSet.getString("name"));
          assertFalse(resultSet.next());
        }

        // There are no more results. This is indicated by getMoreResults returning false AND
        // getUpdateCount returning -1.
        assertFalse(statement.getMoreResults());
        assertEquals(-1, statement.getUpdateCount());
      }
    }
  }
}
