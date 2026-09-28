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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import com.google.spanner.v1.TypeCode;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Base64;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.PGStatement;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class ArraysMockServerTest extends AbstractMockServerTest {
  private static com.google.spanner.v1.ResultSet createResultSet(
      String columnName, TypeCode arrayElementType, Iterable<Value> values) {
    return createResultSet(
        columnName, arrayElementType, TypeAnnotationCode.TYPE_ANNOTATION_CODE_UNSPECIFIED, values);
  }

  private static com.google.spanner.v1.ResultSet createResultSet(
      String columnName,
      TypeCode arrayElementType,
      TypeAnnotationCode arrayElementAnnotationCode,
      Iterable<Value> values) {
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
                                        Type.newBuilder()
                                            .setCode(arrayElementType)
                                            .setTypeAnnotation(arrayElementAnnotationCode)
                                            .build())
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "BOOL_ARRAY",
                TypeCode.BOOL,
                ImmutableList.of(Values.of(true), Values.ofNull(), Values.of(false)))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BOOL_ARRAY");
        assertArrayEquals(new Boolean[] {true, null, false}, (Boolean[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testBigintArrayInResultSet() throws SQLException {
    String sql = "SELECT BIGINT_ARRAY FROM FOO";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "BIGINT_ARRAY",
                TypeCode.INT64,
                ImmutableList.of(Values.of("1"), Values.ofNull(), Values.of("2")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BIGINT_ARRAY");
        assertArrayEquals(new Long[] {1L, null, 2L}, (Long[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testBigintArrayBinary() throws SQLException {
    String sql = "SELECT BIGINT_ARRAY FROM FOO WHERE BAR=?";
    String spannerSql = sql.replace("?", "$1");
    com.google.spanner.v1.ResultSet spannerResultSet =
        createResultSet(
            "BIGINT_ARRAY",
            TypeCode.INT64,
            ImmutableList.of(Values.of("1"), Values.ofNull(), Values.of("2")));
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(spannerSql), spannerResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(spannerSql).bind("p1").to(1L).build(), spannerResultSet));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?binaryTransferEnable=" + Oid.INT8_ARRAY)) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        statement.unwrap(PGStatement.class).setPrepareThreshold(-1);
        statement.setLong(1, 1L);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          Array array = resultSet.getArray("BIGINT_ARRAY");
          assertArrayEquals(new Long[] {1L, null, 2L}, (Long[]) array.getArray());
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testBigintArrayBinary_EmptyArray() throws SQLException {
    String sql = "SELECT BIGINT_ARRAY FROM FOO WHERE BAR=?";
    String spannerSql = sql.replace("?", "$1");
    com.google.spanner.v1.ResultSet spannerResultSet =
        createResultSet("BIGINT_ARRAY", TypeCode.INT64, ImmutableList.of());
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(spannerSql), spannerResultSet));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(spannerSql).bind("p1").to(1L).build(), spannerResultSet));

    try (Connection connection =
        DriverManager.getConnection(createUrl() + "?binaryTransferEnable=" + Oid.INT8_ARRAY)) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        statement.unwrap(PGStatement.class).setPrepareThreshold(-1);
        statement.setLong(1, 1L);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertTrue(resultSet.next());
          Array array = resultSet.getArray("BIGINT_ARRAY");
          assertArrayEquals(new Long[] {}, (Long[]) array.getArray());
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void testBindEmptyBinaryArrayViaWireProtocol() throws Exception {
    String sql = "SELECT 1 WHERE $1 = '{}'::bigint[]";
    String spannerSql = "SELECT 1 WHERE $1 = '{}'::bigint[]";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(spannerSql).bind("p1").toInt64Array(new long[] {}).build(),
            com.google.spanner.v1.ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("col")
                                        .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                                        .build())
                                .build())
                        .build())
                .addRows(ListValue.newBuilder().addValues(Values.of(1L)).build())
                .build()));

    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      outputStream.writeInt(29);
      outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
      outputStream.writeBytes("user");
      outputStream.writeByte(0);
      outputStream.writeBytes("foo");
      outputStream.writeByte(0);
      outputStream.writeBytes("database");
      outputStream.writeByte(0);
      outputStream.writeBytes("d");
      outputStream.writeByte(0);
      outputStream.writeByte(0);
      outputStream.flush();

      while (true) {
        byte message = inputStream.readByte();
        int length = inputStream.readInt();
        inputStream.readFully(new byte[length - 4]);
        if (message == 'Z') {
          break;
        }
      }

      // 1. Parse message: Parse("", sql, [Oid.INT8_ARRAY])
      byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
      outputStream.writeByte('P');
      outputStream.writeInt(4 + 1 + sqlBytes.length + 1 + 2 + 4);
      outputStream.writeByte(0); // unnamed statement
      outputStream.write(sqlBytes);
      outputStream.writeByte(0);
      outputStream.writeShort(1); // 1 parameter type
      outputStream.writeInt(Oid.INT8_ARRAY);

      // 2. Bind message: Bind("", "", [format=1 (binary)], [12-byte empty array], [resultFormat=0])
      byte[] emptyArrayBytes = new byte[12];
      ByteConverter.int4(emptyArrayBytes, 0, 0); // 0 dimensions
      ByteConverter.int4(emptyArrayBytes, 4, 0); // null flag = 0
      ByteConverter.int4(emptyArrayBytes, 8, Oid.INT8); // oid = 20 (INT8)

      outputStream.writeByte('B');
      outputStream.writeInt(4 + 1 + 1 + 2 + 2 + 2 + 4 + emptyArrayBytes.length + 2);
      outputStream.writeByte(0); // unnamed portal
      outputStream.writeByte(0); // unnamed statement
      outputStream.writeShort(1); // 1 format code
      outputStream.writeShort(1); // binary format
      outputStream.writeShort(1); // 1 parameter
      outputStream.writeInt(emptyArrayBytes.length);
      outputStream.write(emptyArrayBytes);
      outputStream.writeShort(0); // 0 result format codes (default text)

      // 3. Execute message: Execute("", 0)
      outputStream.writeByte('E');
      outputStream.writeInt(4 + 1 + 4);
      outputStream.writeByte(0); // unnamed portal
      outputStream.writeInt(0); // fetch all

      // 4. Sync message
      outputStream.writeByte('S');
      outputStream.writeInt(4);
      outputStream.flush();

      // Read responses: ParseComplete ('1'), BindComplete ('2'), DataRow ('D'), CommandComplete
      // ('C'), ReadyForQuery ('Z')
      assertEquals('1', inputStream.readByte());
      assertEquals(4, inputStream.readInt());

      assertEquals('2', inputStream.readByte());
      assertEquals(4, inputStream.readInt());

      assertEquals('D', inputStream.readByte());
      int dataRowLength = inputStream.readInt();
      inputStream.readFully(new byte[dataRowLength - 4]);

      assertEquals('C', inputStream.readByte());
      int commandCompleteLength = inputStream.readInt();
      inputStream.readFully(new byte[commandCompleteLength - 4]);

      assertEquals('Z', inputStream.readByte());
      assertEquals(5, inputStream.readInt());
      assertEquals('I', inputStream.readByte());
    }
  }

  @Test
  public void testDoubleArrayInResultSet() throws SQLException {
    String sql = "SELECT DOUBLE_ARRAY FROM FOO";
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
                    Values.ofNull(),
                    Values.of(Double.NaN)))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("DOUBLE_ARRAY");
        assertArrayEquals(
            new Double[] {3.14d, Double.MAX_VALUE, Double.MIN_VALUE, null, Double.NaN},
            (Double[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testNumericArrayInResultSet() throws SQLException {
    String sql = "SELECT NUMERIC_ARRAY FROM FOO";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "NUMERIC_ARRAY",
                TypeCode.NUMERIC,
                TypeAnnotationCode.PG_NUMERIC,
                ImmutableList.of(
                    Values.of("3.14"),
                    Values.of("1000.0"),
                    Values.ofNull(),
                    Values.of("-0.123456789")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("NUMERIC_ARRAY");
        assertArrayEquals(
            new BigDecimal[] {
              new BigDecimal("3.14"), new BigDecimal("1000.0"), null, new BigDecimal("-0.123456789")
            },
            (BigDecimal[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testVarcharArrayInResultSet() throws SQLException {
    String sql = "SELECT VARCHAR_ARRAY FROM FOO";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "VARCHAR_ARRAY",
                TypeCode.STRING,
                ImmutableList.of(Values.of("test1"), Values.ofNull(), Values.of("test2")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("VARCHAR_ARRAY");
        assertArrayEquals(new String[] {"test1", null, "test2"}, (String[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testByteaArrayInResultSet() throws SQLException {
    String sql = "SELECT BYTEA_ARRAY FROM FOO";
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
                    Values.ofNull(),
                    Values.of(
                        Base64.getEncoder()
                            .encodeToString("test2".getBytes(StandardCharsets.UTF_8)))))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("BYTEA_ARRAY");
        assertArrayEquals(
            new byte[][] {
              "test1".getBytes(StandardCharsets.UTF_8),
              null,
              "test2".getBytes(StandardCharsets.UTF_8)
            },
            (byte[][]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testDateArrayInResultSet() throws SQLException {
    String sql = "SELECT DATE_ARRAY FROM FOO";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "DATE_ARRAY",
                TypeCode.DATE,
                ImmutableList.of(
                    Values.of("2022-02-14"), Values.ofNull(), Values.of("2000-02-29")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("DATE_ARRAY");
        assertArrayEquals(
            new Date[] {Date.valueOf("2022-02-14"), null, Date.valueOf("2000-02-29")},
            (Date[]) array.getArray());
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testTimestampArrayInResultSet() throws SQLException {
    String sql = "SELECT TIMESTAMP_ARRAY FROM FOO";
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(sql),
            createResultSet(
                "TIMESTAMP_ARRAY",
                TypeCode.TIMESTAMP,
                ImmutableList.of(
                    Values.of("2022-02-14T11:47:10.123456700Z"),
                    Values.ofNull(),
                    Values.of("2000-02-29T00:00:01.00000100Z")))));

    try (Connection connection = DriverManager.getConnection(createUrl())) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
        assertTrue(resultSet.next());
        Array array = resultSet.getArray("TIMESTAMP_ARRAY");
        Timestamp[] expected =
            new Timestamp[] {
              com.google.cloud.Timestamp.parseTimestamp("2022-02-14T11:47:10.123456Z")
                  .toSqlTimestamp(),
              null,
              com.google.cloud.Timestamp.parseTimestamp("2000-02-29T00:00:01.000001Z")
                  .toSqlTimestamp()
            };
        Timestamp[] actual = (Timestamp[]) array.getArray();
        assertArrayEquals(expected, actual);
        assertFalse(resultSet.next());
      }
    }
  }
}
