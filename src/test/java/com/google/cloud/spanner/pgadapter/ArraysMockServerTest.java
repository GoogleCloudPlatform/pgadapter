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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Base64;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArraysMockServerTest extends AbstractMockServerTest {
  private static com.google.spanner.v1.ResultSet createResultSet(
      String columnName, TypeCode arrayElementType, Iterable<Value> values) {
    com.google.spanner.v1.ResultSet.Builder builder = com.google.spanner.v1.ResultSet.newBuilder();
    builder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName(columnName)
                            .setType(
                                Type.newBuilder()
                                    .setCode(TypeCode.ARRAY)
                                    .setArrayElementType(
                                        Type.newBuilder().setCode(arrayElementType).build())
                                    .build())
                            .build())
                    .build())
            .build());
    builder.addRows(ListValue.newBuilder().addValues(Values.of(values)).build());
    return builder.build();
  }

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/", pgServer.getLocalPort());
  }

  @Test
  public void testBooleanArrayInResultSet() throws SQLException {
    String sql = "SELECT BOOL_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "BOOL_ARRAY", TypeCode.BOOL, ImmutableList.of(Values.of(true), Values.of(false)))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BOOL_ARRAY");
        assertArrayEquals(new Boolean[] {true, false}, (Boolean[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testBigintArrayInResultSet() throws SQLException {
    String sql = "SELECT BIGINT_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "BIGINT_ARRAY", TypeCode.INT64, ImmutableList.of(Values.of("1"), Values.of("2")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BIGINT_ARRAY");
        assertArrayEquals(new Long[] {1L, 2L}, (Long[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testDoubleArrayInResultSet() throws SQLException {
    String sql = "SELECT DOUBLE_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "DOUBLE_ARRAY",
                TypeCode.FLOAT64,
                ImmutableList.of(
                    Values.of(3.14d),
                    Values.of(Double.MAX_VALUE),
                    Values.of(Double.MIN_VALUE),
                    Values.of(Double.NaN)))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("DOUBLE_ARRAY");
        assertArrayEquals(
            new Double[] {3.14d, Double.MAX_VALUE, Double.MIN_VALUE, Double.NaN},
            (Double[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNumericArrayInResultSet() throws SQLException {
    String sql = "SELECT NUMERIC_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "NUMERIC_ARRAY",
                TypeCode.NUMERIC,
                ImmutableList.of(
                    Values.of("3.14"), Values.of("1000.0"), Values.of("-0.123456789")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("NUMERIC_ARRAY");
        assertArrayEquals(
            new BigDecimal[] {
              new BigDecimal("3.14"), new BigDecimal("1000.0"), new BigDecimal("-0.123456789")
            },
            (BigDecimal[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testVarcharArrayInResultSet() throws SQLException {
    String sql = "SELECT VARCHAR_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "VARCHAR_ARRAY",
                TypeCode.STRING,
                ImmutableList.of(Values.of("test1"), Values.of("test2")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("VARCHAR_ARRAY");
        assertArrayEquals(new String[] {"test1", "test2"}, (String[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testByteaArrayInResultSet() throws SQLException {
    String sql = "SELECT BYTEA_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "BYTEA_ARRAY",
                TypeCode.BYTES,
                ImmutableList.of(
                    Values.of(
                        Base64.getEncoder()
                            .encodeToString("test1".getBytes(StandardCharsets.UTF_8))),
                    Values.of(
                        Base64.getEncoder()
                            .encodeToString("test2".getBytes(StandardCharsets.UTF_8)))))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BYTEA_ARRAY");
        assertArrayEquals(
            new byte[][] {
              "test1".getBytes(StandardCharsets.UTF_8), "test2".getBytes(StandardCharsets.UTF_8)
            },
            (byte[][]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testDateArrayInResultSet() throws SQLException {
    String sql = "SELECT DATE_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "DATE_ARRAY",
                TypeCode.DATE,
                ImmutableList.of(Values.of("2022-02-14"), Values.of("2000-02-29")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("DATE_ARRAY");
        assertArrayEquals(
            new Date[] {Date.valueOf("2022-02-14"), Date.valueOf("2000-02-29")},
            (Date[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTimestampArrayInResultSet() throws SQLException {
    String sql = "SELECT TIMESTAMP_ARRAY FROM FOO";
    // TODO: Add NULL element to the array once null values are properly supported
    // See https://github.com/GoogleCloudPlatform/pgadapter/pull/35
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "TIMESTAMP_ARRAY",
                TypeCode.TIMESTAMP,
                ImmutableList.of(
                    Values.of("2022-02-14T11:47:10.123456700Z"),
                    Values.of("2000-02-29T00:00:01.00000100Z")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("TIMESTAMP_ARRAY");
        assertArrayEquals(
            new Timestamp[] {
              com.google.cloud.Timestamp.parseTimestamp("2022-02-14T11:47:10.123456700Z")
                  .toSqlTimestamp(),
              com.google.cloud.Timestamp.parseTimestamp("2000-02-29T00:00:01.00000100Z")
                  .toSqlTimestamp()
            },
            (Timestamp[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }
}
