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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.postgresql.core.Oid;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;

@Category({IntegrationTest.class})
@RunWith(Parameterized.class)
public class ITJdbcTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @Parameter public String preferQueryMode;

  @Parameter(1)
  public boolean useDomainSocket;

  @Parameters(name = "preferQueryMode = {0}, useDomainSocket = {1}")
  public static List<Object[]> data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    boolean[] useDomainSockets;
    if (options.isDomainSocketEnabled()) {
      useDomainSockets = new boolean[] {true, false};
    } else {
      useDomainSockets = new boolean[] {false};
    }
    String[] queryModes = {"extended", "simple"};
    List<Object[]> parameters = new ArrayList<>();
    for (String queryMode : queryModes) {
      for (boolean useDomainSocket : useDomainSockets) {
        parameters.add(new Object[] {queryMode, useDomainSocket});
      }
    }
    return parameters;
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
                .set("col_float4")
                .to(3.14f)
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(1)
                .set("col_numeric")
                .to(new BigDecimal("3.14"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00"))
                .set("col_date")
                .to(Date.parseDate("2022-05-23"))
                .set("col_varchar")
                .to("test")
                .set("col_jsonb")
                .to("{\"key\": \"value\"}")
                .set("col_array_bigint")
                .toInt64Array(Arrays.asList(1L, null, 2L))
                .set("col_array_bool")
                .toBoolArray(Arrays.asList(true, null, false))
                .set("col_array_bytea")
                .toBytesArray(
                    Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
                .set("col_array_float4")
                .toFloat32Array(Arrays.asList(3.14f, null, -99.8f))
                .set("col_array_float8")
                .toFloat64Array(Arrays.asList(3.14d, null, -99.8))
                .set("col_array_int")
                .toInt64Array(Arrays.asList(-1L, null, -2L))
                .set("col_array_numeric")
                .toPgNumericArray(Arrays.asList("6.626", null, "-3.14"))
                .set("col_array_timestamptz")
                .toTimestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z"),
                        null,
                        Timestamp.parseTimestamp("1970-01-01T00:00:00Z")))
                .set("col_array_date")
                .toDateArray(
                    Arrays.asList(Date.parseDate("2000-01-01"), null, Date.parseDate("1970-01-01")))
                .set("col_array_varchar")
                .toStringArray(Arrays.asList("string1", null, "string2"))
                .set("col_array_jsonb")
                .toPgJsonbArray(
                    Arrays.asList("{\"key\": \"value1\"}", null, "{\"key\": \"value2\"}"))
                .build()));
  }

  private String getConnectionUrl() {
    if (useDomainSocket) {
      return String.format(
          "jdbc:postgresql://localhost/%s?"
              + "preferQueryMode=%s"
              + "&socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg"
              + "&socketFactoryArg=/tmp/.s.PGSQL.%d",
          database.getId().getDatabase(), preferQueryMode, testEnv.getPGAdapterPort());
    }
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
  public void testSelectParameterizedOffsetWithoutLimit() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      connection.createStatement().execute("set spanner.auto_add_limit_clause=true");
      try (PreparedStatement statement =
          connection.prepareStatement("select * from numbers offset ?")) {
        for (long offset : new long[] {0L, 1L}) {
          statement.setLong(1, offset);
          try (ResultSet resultSet = statement.executeQuery()) {
            if (offset == 0L) {
              assertTrue(resultSet.next());
              assertEquals(1L, resultSet.getLong(1));
              assertFalse(resultSet.next());
            } else {
              assertFalse(resultSet.next());
            }
          }
        }
      }
    }
  }

  @Test
  public void testSelectCurrentSchema() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select current_schema()")) {
        assertTrue(resultSet.next());
        assertEquals("public", resultSet.getString(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPgCatalogViews() throws SQLException {
    for (String view :
        new String[] {"pg_sequence", "pg_sequences", "pg_description", "pg_index", "pg_language"}) {
      for (String prefix : new String[] {"", "pg_catalog."}) {
        try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
          try (ResultSet resultSet =
              connection.createStatement().executeQuery("SELECT * FROM " + prefix + view)) {
            while (resultSet.next()) {
              assertNotNull(resultSet.getMetaData());
              assertNotEquals(0, resultSet.getMetaData().getColumnCount());
            }
          }
        }
      }
    }
  }

  @Test
  public void testInformationSchemaViews() throws SQLException {
    for (String view : new String[] {"sequences"}) {
      try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("SELECT * FROM INFORMATION_SCHEMA." + view)) {
          while (resultSet.next()) {
            assertNotNull(resultSet.getMetaData());
            assertNotEquals(0, resultSet.getMetaData().getColumnCount());
          }
        }
      }
    }
  }

  @Test
  public void testServerVersionNum() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("SELECT current_setting('server_version_num')")) {
        assertTrue(resultSet.next());
        assertEquals(140001, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testCreateTableIfNotExists() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (Statement statement = connection.createStatement()) {
        // This table already exists, so it should be a no-op.
        assertFalse(
            statement.execute("create table if not exists all_types (id bigint primary key)"));
        assertFalse(statement.getMoreResults());
      }
    }
  }

  @Test
  public void testCreateTableIfNotExists_withSchemaPrefix() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (Statement statement = connection.createStatement()) {
        // This table already exists, so it should be a no-op.
        assertFalse(
            statement.execute(
                "create table if not exists public.all_types (id bigint primary key)"));
        assertFalse(statement.getMoreResults());
      }
    }
  }

  @Test
  public void testSelectWithParameters() throws SQLException {
    boolean isSimpleMode = "simple".equalsIgnoreCase(preferQueryMode);
    String sql =
        "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
            + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float8, col_array_int, "
            + "col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb "
            + "from all_types "
            + "where col_bigint=? "
            + "and col_bool=? "
            // The PG JDBC driver does not support bytea parameters in simple mode.
            + (isSimpleMode ? "" : "and col_bytea=? ")
            + "and col_float8=? "
            + "and col_int=? "
            + "and col_numeric=? "
            + "and col_timestamptz=? "
            + "and col_date=? "
            + "and col_varchar=? "
            + "and col_jsonb::text=?::text";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {

        int index = 0;
        statement.setLong(++index, 1);
        statement.setBoolean(++index, true);
        if (!isSimpleMode) {
          statement.setBytes(++index, "test".getBytes(StandardCharsets.UTF_8));
        }
        statement.setDouble(++index, 3.14d);
        // TODO: Remove when Spangres supports casting to int4
        if (isSimpleMode) {
          statement.setLong(++index, 1);
        } else {
          statement.setInt(++index, 1);
        }
        statement.setBigDecimal(++index, new BigDecimal("3.14"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp());
        statement.setObject(++index, LocalDate.of(2022, 5, 23));
        statement.setString(++index, "test");
        statement.setString(++index, "{\"key\": \"value\"}");

        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());

          index = 0;
          assertEquals(1, resultSet.getLong(++index));
          assertTrue(resultSet.getBoolean(++index));
          if (!isSimpleMode) {
            assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
          } else {
            ++index;
          }
          assertEquals(3.14d, resultSet.getDouble(++index), 0.0d);
          assertEquals(1, resultSet.getInt(++index));
          assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(++index));
          assertEquals(
              Timestamp.parseTimestamp("2022-01-27T17:51:30+01:00").toSqlTimestamp(),
              resultSet.getTimestamp(++index));
          assertEquals(
              LocalDate.parse("2022-05-23"), resultSet.getObject(++index, LocalDate.class));
          assertEquals("test", resultSet.getString(++index));
          assertEquals("{\"key\": \"value\"}", resultSet.getString(++index));

          assertArrayEquals(
              new Long[] {1L, null, 2L}, (Long[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new Boolean[] {true, null, false},
              (Boolean[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new byte[][] {
                "bytes1".getBytes(StandardCharsets.UTF_8),
                null,
                "bytes2".getBytes(StandardCharsets.UTF_8)
              },
              (byte[][]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new Double[] {3.14d, null, -99.8}, (Double[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new Long[] {-1L, null, -2L}, (Long[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new BigDecimal[] {new BigDecimal("6.626"), null, new BigDecimal("-3.14")},
              (BigDecimal[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new java.sql.Timestamp[] {
                Timestamp.parseTimestamp("2000-01-01T00:00:00Z").toSqlTimestamp(),
                null,
                Timestamp.parseTimestamp("1970-01-01T00:00:00Z").toSqlTimestamp()
              },
              (java.sql.Timestamp[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new java.sql.Date[] {
                java.sql.Date.valueOf("2000-01-01"), null, java.sql.Date.valueOf("1970-01-01")
              },
              (java.sql.Date[]) resultSet.getArray(++index).getArray());
          assertArrayEquals(
              new String[] {"string1", null, "string2"},
              (String[]) resultSet.getArray(++index).getArray());
          // TODO: Remove when Spangres supports casting to int4
          if (!isSimpleMode) {
            assertArrayEquals(
                new String[] {"{\"key\": \"value1\"}", null, "{\"key\": \"value2\"}"},
                (String[]) resultSet.getArray(++index).getArray());
          }

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
                  + "(col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb, "
                  + "col_array_bigint, col_array_bool, col_array_bytea, col_array_float4, col_array_float8, col_array_int, col_array_numeric, col_array_timestamptz, col_array_date, col_array_varchar, col_array_jsonb) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setBoolean(++index, true);
        // The PG JDBC driver does not support bytea parameters in simple mode.
        if (isSimpleMode) {
          statement.setNull(++index, Types.BINARY);
        } else {
          statement.setBytes(++index, "bytes_test".getBytes(StandardCharsets.UTF_8));
        }
        statement.setFloat(++index, 10.1f);
        statement.setDouble(++index, 10.1);
        // TODO: Remove when the emulator supports casting to int4
        if (isSimpleMode) {
          statement.setLong(++index, 100);
        } else {
          statement.setInt(++index, 100);
        }
        statement.setBigDecimal(++index, new BigDecimal("6.626"));
        statement.setTimestamp(
            ++index, Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp());
        statement.setObject(++index, LocalDate.parse("2000-02-29"));
        statement.setString(++index, "string_test");
        statement.setObject(++index, "{\"key1\": \"value1\", \"key2\": \"value2\"}", Types.OTHER);

        statement.setArray(++index, connection.createArrayOf("bigint", new Long[] {1L, null, 2L}));
        statement.setArray(
            ++index, connection.createArrayOf("bool", new Boolean[] {true, null, false}));
        if (isSimpleMode) {
          statement.setNull(++index, Types.ARRAY);
        } else {
          statement.setArray(
              ++index,
              connection.createArrayOf(
                  "bytea",
                  new byte[][] {
                    "bytes1".getBytes(StandardCharsets.UTF_8),
                    null,
                    "bytes2".getBytes(StandardCharsets.UTF_8)
                  }));
        }
        statement.setArray(
            ++index, connection.createArrayOf("float4", new Float[] {3.14f, null, -99.8f}));
        statement.setArray(
            ++index, connection.createArrayOf("float8", new Double[] {3.14d, null, -99.8}));
        // TODO: Remove when Spangres supports casting to int4
        statement.setArray(
            ++index,
            connection.createArrayOf(
                isSimpleMode ? "bigint" : "int", new Integer[] {-1, null, -2}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "numeric",
                new BigDecimal[] {new BigDecimal("6.626"), null, new BigDecimal("-3.14")}));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "timestamptz",
                new java.sql.Timestamp[] {
                  Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp(),
                  null,
                  Timestamp.parseTimestamp("2000-01-01T00:00:00Z").toSqlTimestamp()
                }));
        statement.setArray(
            ++index,
            connection.createArrayOf(
                "date",
                new LocalDate[] {LocalDate.of(2000, 1, 1), null, LocalDate.of(1970, 1, 1)}));
        statement.setArray(
            ++index,
            connection.createArrayOf("varchar", new String[] {"string1", null, "string2"}));
        // TODO: Remove when the emulator supports casting to int4
        if (!isSimpleMode) {
          statement.setArray(
              ++index,
              connection.createArrayOf(
                  "jsonb",
                  new String[] {
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                    null,
                    "{\"key1\": \"value3\", \"key2\": \"value4\"}"
                  }));
        } else {
          statement.setArray(
              ++index,
              connection.createArrayOf(
                  "varchar",
                  new String[] {
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                    null,
                    "{\"key1\": \"value3\", \"key2\": \"value4\"}"
                  }));
        }

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=2")) {
        assertTrue(resultSet.next());

        assertEquals(22, resultSet.getMetaData().getColumnCount());
        int index = 0;
        assertEquals(2, resultSet.getLong(++index));
        assertTrue(resultSet.getBoolean(++index));
        if (!isSimpleMode) {
          assertArrayEquals(
              "bytes_test".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
        } else {
          ++index;
        }
        assertEquals(10.1f, resultSet.getFloat(++index), 0.0f);
        assertEquals(10.1d, resultSet.getDouble(++index), 0.0d);
        assertEquals(100, resultSet.getInt(++index));
        assertEquals(new BigDecimal("6.626"), resultSet.getBigDecimal(++index));
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T13:45:00.123456+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(++index));
        assertEquals(LocalDate.of(2000, 2, 29), resultSet.getObject(++index, LocalDate.class));
        assertEquals("string_test", resultSet.getString(++index));
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", resultSet.getString(++index));

        assertArrayEquals(
            new Long[] {1L, null, 2L}, (Long[]) resultSet.getArray(++index).getArray());
        assertArrayEquals(
            new Boolean[] {true, null, false}, (Boolean[]) resultSet.getArray(++index).getArray());
        if (isSimpleMode) {
          assertNull(resultSet.getArray(++index));
        } else {
          assertArrayEquals(
              new byte[][] {
                "bytes1".getBytes(StandardCharsets.UTF_8),
                null,
                "bytes2".getBytes(StandardCharsets.UTF_8)
              },
              (byte[][]) resultSet.getArray(++index).getArray());
        }
        assertArrayEquals(
            new Float[] {3.14f, null, -99.8f}, (Float[]) resultSet.getArray(++index).getArray());
        assertArrayEquals(
            new Double[] {3.14d, null, -99.8}, (Double[]) resultSet.getArray(++index).getArray());
        assertArrayEquals(
            new Long[] {-1L, null, -2L}, (Long[]) resultSet.getArray(++index).getArray());
        assertArrayEquals(
            new BigDecimal[] {new BigDecimal("6.626"), null, new BigDecimal("-3.14")},
            (BigDecimal[]) resultSet.getArray(++index).getArray());
        if (isSimpleMode) {
          // Using simple mode will cause the JDBC driver to create a timestamp literal internally.
          // The literal will use the local time zone of the JDK where this test is running, but the
          // literal will not include that time zone offset. Cloud Spanner will treat the timestamp
          // in Pacific Time. This means that the returned time zone could be anything.
          // Note: This is a limitation when using java.sql.Timestamp in combination with simple
          // mode. Using OffsetDateTime and/or extended mode does not have this problem.
          assertNotNull(resultSet.getArray(++index));
        } else {
          assertArrayEquals(
              new java.sql.Timestamp[] {
                Timestamp.parseTimestamp("2022-02-11T12:45:00.123456Z").toSqlTimestamp(),
                null,
                Timestamp.parseTimestamp("2000-01-01T00:00:00Z").toSqlTimestamp()
              },
              (java.sql.Timestamp[]) resultSet.getArray(++index).getArray());
        }
        assertArrayEquals(
            new java.sql.Date[] {
              java.sql.Date.valueOf("2000-01-01"), null, java.sql.Date.valueOf("1970-01-01")
            },
            (java.sql.Date[]) resultSet.getArray(++index).getArray());
        assertArrayEquals(
            new String[] {"string1", null, "string2"},
            (String[]) resultSet.getArray(++index).getArray());
        // TODO: Remove when Spangres supports casting to int4
        if (!isSimpleMode) {
          assertArrayEquals(
              new String[] {
                "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                null,
                "{\"key1\": \"value3\", \"key2\": \"value4\"}"
              },
              (String[]) resultSet.getArray(++index).getArray());
        }

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
            + "col_float4=?, "
            + "col_float8=?, "
            + "col_int=?, "
            + "col_numeric=?, "
            + "col_timestamptz=?, "
            + "col_date=?, "
            + "col_varchar=?, "
            + "col_jsonb=? "
            + "where col_bigint=?";

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        int index = 0;
        statement.setBoolean(++index, false);
        if (!isSimpleMode) {
          statement.setBytes(++index, "updated".getBytes(StandardCharsets.UTF_8));
        }
        statement.setFloat(++index, 3.14f * 2f);
        statement.setDouble(++index, 3.14d * 2d);
        // TODO: Remove when Spangres supports casting to int4
        if (isSimpleMode) {
          statement.setLong(++index, 2);
        } else {
          statement.setInt(++index, 2);
        }
        statement.setBigDecimal(++index, new BigDecimal("10.0"));
        // Note that PostgreSQL does not support nanosecond precision, so the JDBC driver therefore
        // truncates this value before it is sent to PG.
        statement.setTimestamp(
            ++index,
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123456789+01:00").toSqlTimestamp());
        statement.setObject(++index, LocalDate.of(2000, 1, 1));
        statement.setString(++index, "updated");
        PGobject jsonbObject = new PGobject();
        jsonbObject.setType("jsonb");
        jsonbObject.setValue("{\"key1\": \"updated1\", \"key2\": \"updated2\"}");
        statement.setObject(++index, jsonbObject);
        statement.setLong(++index, 1);

        assertEquals(1, statement.executeUpdate());
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from all_types where col_bigint=1")) {
        assertTrue(resultSet.next());

        int index = 0;
        assertEquals(1, resultSet.getLong(++index));
        assertFalse(resultSet.getBoolean(++index));
        if (!isSimpleMode) {
          assertArrayEquals(
              "updated".getBytes(StandardCharsets.UTF_8), resultSet.getBytes(++index));
        } else {
          ++index;
        }
        assertEquals(3.14f * 2f, resultSet.getFloat(++index), 0.0f);
        assertEquals(3.14d * 2d, resultSet.getDouble(++index), 0.0d);
        assertEquals(2, resultSet.getInt(++index));
        assertEquals(new BigDecimal("10.0"), resultSet.getBigDecimal(++index));
        // Note: The JDBC driver already truncated the timestamp value before it was sent to PG.
        // So here we read back the truncated value.
        assertEquals(
            Timestamp.parseTimestamp("2022-02-11T14:04:59.123457+01:00").toSqlTimestamp(),
            resultSet.getTimestamp(++index));
        assertEquals(LocalDate.parse("2000-01-01"), resultSet.getObject(++index, LocalDate.class));
        assertEquals("updated", resultSet.getString(++index));
        assertEquals(
            "{\"key1\": \"updated1\", \"key2\": \"updated2\"}", resultSet.getString(++index));

        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNullValues() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              "insert into all_types "
                  + "(col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) "
                  + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        int index = 0;
        statement.setLong(++index, 2);
        statement.setNull(++index, Types.BOOLEAN);
        statement.setNull(++index, Types.BINARY);
        statement.setNull(++index, Types.DOUBLE);
        statement.setNull(++index, Types.INTEGER);
        statement.setNull(++index, Types.NUMERIC);
        statement.setNull(++index, Types.TIMESTAMP_WITH_TIMEZONE);
        statement.setNull(++index, Types.DATE);
        statement.setNull(++index, Types.VARCHAR);
        statement.setNull(++index, Types.OTHER);

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
        assertNull(resultSet.getDate(++index));
        assertTrue(resultSet.wasNull());
        assertNull(resultSet.getString(++index));
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

  @Test
  public void testRelationNotFoundError() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("select * from non_existing_table where id=?")) {
        preparedStatement.setLong(1, 1L);
        PSQLException exception =
            assertThrows(PSQLException.class, preparedStatement::executeQuery);
        if (preferQueryMode.equals("simple")) {
          assertEquals(
              "ERROR: relation \"non_existing_table\" does not exist - Statement: 'select * from non_existing_table where id=('1'::int8)'",
              exception.getMessage());
        } else {
          assertEquals(
              "ERROR: relation \"non_existing_table\" does not exist - Statement: 'select * from non_existing_table where id=$1'",
              exception.getMessage());
        }
        assertEquals(SQLState.UndefinedTable.toString(), exception.getSQLState());
      }
    }
  }

  /**
   * ---------------------------------------------------------------------------------------------/*
   * COPY tests
   *
   * <p>The data that is used for the COPY tests were generated using this statement:
   *
   * <p><code>
   * cat > copy_all_types.sql <<- EOM
   * copy (
   * select (random()*1000000000)::bigint, random()<0.5, md5(random()::text ||
   * clock_timestamp()::text)::bytea, random()*123456789, (random()*999999)::int,
   * (random()*999999)::numeric, now()-random()*interval '500 year', (now()-random()*interval '500 year')::date,
   * md5(random()::text || clock_timestamp()::text)::varchar,
   * ('{"key": "' || md5(random()::text || clock_timestamp()::text)::varchar || '"}')::json
   * from generate_series(1, 10000) s(i)) to stdout;
   * EOM
   * psql -f copy_all_types.sql > all_types_data.txt
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
              "copy all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) from stdin;",
              Files.newInputStream(Paths.get("./src/test/resources/all_types_data_small.txt")));
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
  public void testCopyIn_Nulls() throws SQLException, IOException {
    // Empty all data in the table.
    String databaseId = database.getId().getDatabase();
    testEnv.write(databaseId, Collections.singleton(Mutation.delete("all_types", KeySet.all())));

    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      PGConnection pgConnection = connection.unwrap(PGConnection.class);
      CopyManager copyManager = pgConnection.getCopyAPI();
      long copyCount =
          copyManager.copyIn(
              "copy all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) from stdin;",
              Files.newInputStream(Paths.get("./src/test/resources/all_types_data_nulls.txt")));
      assertEquals(1L, copyCount);

      // Verify that there is 1 row in the table, and that the values of all columns except the
      // primary key are null.
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery(
                  "select col_bigint, col_bool, col_bytea, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb from all_types")) {
        assertTrue(resultSet.next());
        for (int col = 2; col <= resultSet.getMetaData().getColumnCount(); col++) {
          assertNull(String.format("Col %d should be null", col), resultSet.getObject(col));
        }
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
                      "copy all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) from stdin;",
                      Files.newInputStream(Paths.get("./src/test/resources/all_types_data.txt"))));
      assertEquals(
          "ERROR: Record count: 1667 has exceeded the limit: 1666.\n"
              + "\n"
              + "The number of mutations per record is equal to the number of columns in the record plus the number of indexed columns in the record. The maximum number of mutations in one transaction is 20000.\n"
              + "\n"
              + "Execute `SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation to instruct PGAdapter to automatically break large transactions into multiple smaller. This will make the COPY operation non-atomic.\n\n",
          exception.getMessage());
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
              "copy all_types (col_bigint, col_bool, col_bytea, col_float4, col_float8, col_int, col_numeric, col_timestamptz, col_date, col_varchar, col_jsonb) from stdin;",
              Files.newInputStream(Paths.get("./src/test/resources/all_types_data.txt")));
      assertEquals(10_000L, copyCount);

      // Verify that there are 10,000 rows in the table.
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select count(*) from all_types")) {
        assertTrue(resultSet.next());
        assertEquals(10_000L, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }

      // Delete the imported data to prevent the cleanup method to fail on 'Too many mutations'
      // when it tries to delete all data using a normal transaction.
      connection
          .createStatement()
          .execute(
              "delete from all_types"
                  + (IntegrationTest.isRunningOnEmulator() ? " where true" : ""));
    }
  }

  @Test
  public void testTwoDmlStatements() throws SQLException {
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
  public void testDmlAndQueryInBatch() throws SQLException {
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

  @Test
  public void testDml() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (java.sql.PreparedStatement statement =
          connection.prepareStatement("INSERT INTO numbers VALUES (?, ?)")) {
        statement.setLong(1, 2L);
        statement.setString(2, "Two");
        statement.addBatch();

        statement.setLong(1, 3L);
        statement.setString(2, "Three");
        statement.addBatch();

        statement.setLong(1, 4L);
        statement.setString(2, "Four");
        statement.addBatch();

        int[] updateCounts = statement.executeBatch();
        assertArrayEquals(new int[] {1, 1, 1}, updateCounts);

        // Read back the data to verify.
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("SELECT name FROM numbers ORDER BY num")) {
          assertTrue(resultSet.next());
          assertEquals("One", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Two", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Three", resultSet.getString("name"));
          assertTrue(resultSet.next());
          assertEquals("Four", resultSet.getString("name"));
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testFetchSize() throws SQLException {
    writeExtraTestRows();
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // Fetch size is only respected in a transaction.
      connection.setAutoCommit(false);
      try (PreparedStatement statement =
          connection.prepareStatement("select * from all_types order by col_bigint")) {
        // Fetch two rows at a time from the PG server.
        statement.setFetchSize(2);
        try (ResultSet resultSet = statement.executeQuery()) {
          int rowCount = 0;
          while (resultSet.next()) {
            assertEquals(++rowCount, resultSet.getLong(1));
            for (int col = 2; col <= resultSet.getMetaData().getColumnCount(); col++) {
              if (resultSet.getLong(1) == 3L) {
                assertNull(resultSet.getObject(col));
              } else {
                // JSONB (array) is currently not supported using the PG JDBC driver with the
                // emulator, as the PG JDBC driver tries to dynamically load information about the
                // type from teh pg_catalog tables.
                if (!(resultSet.getMetaData().getColumnName(col).equals("col_jsonb")
                    || resultSet.getMetaData().getColumnName(col).equals("col_array_jsonb"))) {
                  assertNotNull(
                      "Column " + resultSet.getMetaData().getColumnName(col),
                      resultSet.getObject(col));
                }
              }
            }
          }
          assertEquals(5, rowCount);
        }
      }
      connection.commit();
    }
  }

  @Test
  public void testPGSettings() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      // First verify the default value.
      // JDBC sets the DateStyle to 'ISO' for every connection in the connection request, except in
      // version 42.7.0, where the value is 'ISO, MDY'.
      String originalDateStyle;
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select setting from pg_settings where name='DateStyle'")) {
        assertTrue(resultSet.next());
        originalDateStyle = resultSet.getString("setting");
        assertTrue(
            "Original value: " + originalDateStyle,
            "ISO".equals(originalDateStyle) || "ISO, MDY".equals(originalDateStyle));
        assertFalse(resultSet.next());
      }
      // Verify that we can also use a statement parameter to query the pg_settings table.
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("select setting from pg_settings where name=?")) {
        preparedStatement.setString(1, "DateStyle");
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertTrue(resultSet.next());
          String dateStyle = resultSet.getString("setting");
          assertTrue(dateStyle, "ISO".equals(dateStyle) || "ISO, MDY".equals(dateStyle));
          assertFalse(resultSet.next());
        }
      }
      // Change the date style and verify that it is also reflected in  pg_settings.
      connection.createStatement().execute("set datestyle to 'iso, ymd'");
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select setting from pg_settings where name='DateStyle'")) {
        assertTrue(resultSet.next());
        assertEquals("iso, ymd", resultSet.getString("setting"));
        assertFalse(resultSet.next());
      }

      // Verify that pg_settings also respects transactions.
      connection.setAutoCommit(false);
      connection.createStatement().execute("set datestyle to 'iso'");
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select setting from pg_settings where name='DateStyle'")) {
        assertTrue(resultSet.next());
        assertEquals("iso", resultSet.getString("setting"));
        assertFalse(resultSet.next());
      }
      // This should also roll back the changes to pg_settings.
      connection.rollback();
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select setting from pg_settings where name='DateStyle'")) {
        assertTrue(resultSet.next());
        assertEquals("iso, ymd", resultSet.getString("setting"));
        assertFalse(resultSet.next());
      }

      // Resetting the value should bring it back to the initial value.
      connection.createStatement().execute("reset datestyle");
      connection.commit();
      try (ResultSet resultSet =
          connection
              .createStatement()
              .executeQuery("select setting from pg_settings where name='DateStyle'")) {
        assertTrue(resultSet.next());
        assertEquals(originalDateStyle, resultSet.getString("setting"));
        assertFalse(resultSet.next());
      }

      // Verify that we can get all pg_settings and that the number of setting is equal to the
      // expected value.
      int numExpectedSettings = 30;
      int rowCount = 0;
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from pg_settings order by name")) {
        while (resultSet.next()) {
          for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
            // Just verify that we can get the value.
            resultSet.getObject(col);
          }
          assertNotNull(resultSet.getString("name"));
          rowCount++;
        }
      }
      assertEquals(numExpectedSettings, rowCount);
    }
  }

  @Test
  public void testSelectNamespaces() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet namespaces =
          connection
              .createStatement()
              .executeQuery("select nspname from pg_namespace order by oid desc, nspname")) {
        assertTrue(namespaces.next());
        assertEquals("public", namespaces.getString(1));
        assertTrue(namespaces.next());
        assertEquals("pg_catalog", namespaces.getString(1));
        assertTrue(namespaces.next());
        assertEquals("information_schema", namespaces.getString(1));
        assertTrue(namespaces.next());
        assertEquals("spanner_sys", namespaces.getString(1));

        assertFalse(namespaces.next());
      }
    }
  }

  @Test
  public void testSelectTypes() throws SQLException {
    try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
      try (ResultSet types =
          connection
              .createStatement()
              .executeQuery("select oid, typname from pg_type order by oid")) {
        assertTrue(types.next());
        assertEquals(Oid.BOOL, types.getInt(1));
        assertEquals("bool", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.BYTEA, types.getInt(1));
        assertEquals("bytea", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT8, types.getInt(1));
        assertEquals("int8", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT2, types.getInt(1));
        assertEquals("int2", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT4, types.getInt(1));
        assertEquals("int4", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TEXT, types.getInt(1));
        assertEquals("text", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.FLOAT4, types.getInt(1));
        assertEquals("float4", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.FLOAT8, types.getInt(1));
        assertEquals("float8", types.getString(2));
        assertTrue(types.next());
        assertEquals(705, types.getInt(1));
        assertEquals("unknown", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.BOOL_ARRAY, types.getInt(1));
        assertEquals("_bool", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.BYTEA_ARRAY, types.getInt(1));
        assertEquals("_bytea", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT2_ARRAY, types.getInt(1));
        assertEquals("_int2", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT4_ARRAY, types.getInt(1));
        assertEquals("_int4", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TEXT_ARRAY, types.getInt(1));
        assertEquals("_text", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.VARCHAR_ARRAY, types.getInt(1));
        assertEquals("_varchar", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.INT8_ARRAY, types.getInt(1));
        assertEquals("_int8", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.FLOAT4_ARRAY, types.getInt(1));
        assertEquals("_float4", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.FLOAT8_ARRAY, types.getInt(1));
        assertEquals("_float8", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.VARCHAR, types.getInt(1));
        assertEquals("varchar", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.DATE, types.getInt(1));
        assertEquals("date", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TIMESTAMP, types.getInt(1));
        assertEquals("timestamp", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TIMESTAMP_ARRAY, types.getInt(1));
        assertEquals("_timestamp", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.DATE_ARRAY, types.getInt(1));
        assertEquals("_date", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TIMESTAMPTZ, types.getInt(1));
        assertEquals("timestamptz", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.TIMESTAMPTZ_ARRAY, types.getInt(1));
        assertEquals("_timestamptz", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.NUMERIC_ARRAY, types.getInt(1));
        assertEquals("_numeric", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.NUMERIC, types.getInt(1));
        assertEquals("numeric", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.JSONB, types.getInt(1));
        assertEquals("jsonb", types.getString(2));
        assertTrue(types.next());
        assertEquals(Oid.JSONB_ARRAY, types.getInt(1));
        assertEquals("_jsonb", types.getString(2));

        assertFalse(types.next());
      }
    }
  }

  private void writeExtraTestRows() {
    testEnv.write(
        database.getId().getDatabase(),
        Arrays.asList(
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(2L)
                .set("col_bool")
                .to(false)
                .set("col_bytea")
                .to(ByteArray.copyFrom("foo"))
                .set("col_float4")
                .to(-3.14f)
                .set("col_float8")
                .to(-3.14d)
                .set("col_int")
                .to(Integer.MAX_VALUE)
                .set("col_numeric")
                .to(new BigDecimal("-3.14"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-04-22T19:27:30+02:00"))
                .set("col_date")
                .to(Date.parseDate("2000-01-01"))
                .set("col_varchar")
                .to("bar")
                .set("col_jsonb")
                .to(Value.pgJsonb("{\"key\": \"value2\"}"))
                .set("col_array_bigint")
                .toInt64Array(Arrays.asList(1L, null, 2L))
                .set("col_array_bool")
                .toBoolArray(Arrays.asList(true, null, false))
                .set("col_array_bytea")
                .toBytesArray(
                    Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")))
                .set("col_array_float4")
                .toFloat32Array(Arrays.asList(3.14f, null, -99.8f))
                .set("col_array_float8")
                .toFloat64Array(Arrays.asList(3.14d, null, -99.8))
                .set("col_array_int")
                .toInt64Array(Arrays.asList(-1L, null, -2L))
                .set("col_array_numeric")
                .toPgNumericArray(Arrays.asList("6.626", null, "-3.14"))
                .set("col_array_timestamptz")
                .toTimestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z"),
                        null,
                        Timestamp.parseTimestamp("1970-01-01T00:00:00Z")))
                .set("col_array_date")
                .toDateArray(
                    Arrays.asList(Date.parseDate("2000-01-01"), null, Date.parseDate("1970-01-01")))
                .set("col_array_varchar")
                .toStringArray(Arrays.asList("string1", null, "string2"))
                .set("col_array_jsonb")
                .toPgJsonbArray(
                    Arrays.asList("{\"key\": \"value1\"}", null, "{\"key\": \"value2\"}"))
                .build(),
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(3L)
                .set("col_bool")
                .to((Boolean) null)
                .set("col_bytea")
                .to((ByteArray) null)
                .set("col_float4")
                .to((Float) null)
                .set("col_float8")
                .to((Double) null)
                .set("col_int")
                .to((Long) null)
                .set("col_numeric")
                .to((BigDecimal) null)
                .set("col_timestamptz")
                .to((Timestamp) null)
                .set("col_date")
                .to((Date) null)
                .set("col_varchar")
                .to((String) null)
                .set("col_jsonb")
                .to(Value.pgJsonb(null))
                .set("col_array_bigint")
                .toInt64Array((long[]) null)
                .set("col_array_bool")
                .toBoolArray((boolean[]) null)
                .set("col_array_bytea")
                .toBytesArray(null)
                .set("col_array_float4")
                .toFloat32Array((float[]) null)
                .set("col_array_float8")
                .toFloat64Array((double[]) null)
                .set("col_array_int")
                .toInt64Array((long[]) null)
                .set("col_array_numeric")
                .toPgNumericArray(null)
                .set("col_array_timestamptz")
                .toTimestampArray(null)
                .set("col_array_date")
                .toDateArray(null)
                .set("col_array_varchar")
                .toStringArray(null)
                .set("col_array_jsonb")
                .toPgJsonbArray(null)
                .build(),
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(4L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom(""))
                .set("col_float4")
                .to(0f)
                .set("col_float8")
                .to(0d)
                .set("col_int")
                .to(0)
                .set("col_numeric")
                .to(BigDecimal.ZERO)
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"))
                .set("col_date")
                .to(Date.parseDate("0001-01-01"))
                .set("col_varchar")
                .to("")
                .set("col_jsonb")
                .to(Value.pgJsonb("[]"))
                .set("col_array_bigint")
                .toInt64Array(ImmutableList.of())
                .set("col_array_bool")
                .toBoolArray(ImmutableList.of())
                .set("col_array_bytea")
                .toBytesArray(ImmutableList.of())
                .set("col_array_float4")
                .toFloat32Array(ImmutableList.of())
                .set("col_array_float8")
                .toFloat64Array(ImmutableList.of())
                .set("col_array_int")
                .toInt64Array(ImmutableList.of())
                .set("col_array_numeric")
                .toPgNumericArray(ImmutableList.of())
                .set("col_array_timestamptz")
                .toTimestampArray(ImmutableList.of())
                .set("col_array_date")
                .toDateArray(ImmutableList.of())
                .set("col_array_varchar")
                .toStringArray(ImmutableList.of())
                .set("col_array_jsonb")
                .toPgJsonbArray(ImmutableList.of())
                .build(),
            Mutation.newInsertBuilder("all_types")
                .set("col_bigint")
                .to(5L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom(""))
                .set("col_float4")
                .to(0f)
                .set("col_float8")
                .to(0d)
                .set("col_int")
                .to(0)
                .set("col_numeric")
                .to(BigDecimal.ZERO)
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"))
                .set("col_date")
                .to(Date.parseDate("0001-01-01"))
                .set("col_varchar")
                .to("")
                .set("col_jsonb")
                .to(Value.pgJsonb("{}"))
                .set("col_array_bigint")
                .toInt64Array(ImmutableList.of())
                .set("col_array_bool")
                .toBoolArray(ImmutableList.of())
                .set("col_array_bytea")
                .toBytesArray(ImmutableList.of())
                .set("col_array_float4")
                .toFloat32Array(ImmutableList.of())
                .set("col_array_float8")
                .toFloat64Array(ImmutableList.of())
                .set("col_array_int")
                .toInt64Array(ImmutableList.of())
                .set("col_array_numeric")
                .toPgNumericArray(ImmutableList.of())
                .set("col_array_timestamptz")
                .toTimestampArray(ImmutableList.of())
                .set("col_array_date")
                .toDateArray(ImmutableList.of())
                .set("col_array_varchar")
                .toStringArray(ImmutableList.of())
                .set("col_array_jsonb")
                .toPgJsonbArray(ImmutableList.of())
                .build()));
  }
}
