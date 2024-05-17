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

package com.google.cloud.spanner.pgadapter.parsers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class ArrayParserTest {

  @Test
  public void testInt64() {
    assertEquals(
        "{1,2,3}",
        new ArrayParser(
                "{1,2,3}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
    assertEquals(
        "{1,NULL,3}",
        new ArrayParser(
                "{1,null,3}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
    assertEquals(
        "{1}",
        new ArrayParser(
                "{1}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
    assertEquals(
        "{}",
        new ArrayParser(
                "{}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());

    PGException exception =
        assertThrows(
            PGException.class,
            () ->
                new ArrayParser(
                        "{foo}".getBytes(StandardCharsets.UTF_8),
                        FormatCode.TEXT,
                        mock(SessionState.class),
                        Type.int64(),
                        Oid.INT8)
                    .stringParse());
    assertEquals("Invalid int8 value: foo", exception.getMessage());
    exception =
        assertThrows(
            PGException.class,
            () ->
                new ArrayParser(
                        "{100, bar}".getBytes(StandardCharsets.UTF_8),
                        FormatCode.TEXT,
                        mock(SessionState.class),
                        Type.int64(),
                        Oid.INT8)
                    .stringParse());
    assertEquals("Invalid int8 value: bar", exception.getMessage());
  }

  @Test
  public void testBool() {
    assertEquals(
        Arrays.asList(true, null, false),
        new ArrayParser(
                "{true,null,false}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.bool(),
                Oid.BOOL)
            .item);
    assertEquals(
        Arrays.asList(true, null, false),
        new ArrayParser(
                "{on,null,off}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.bool(),
                Oid.BOOL)
            .item);
    assertEquals(
        Arrays.asList(true, null, false),
        new ArrayParser(
                "{t,null,f}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.bool(),
                Oid.BOOL)
            .item);
  }

  @Test
  public void testBytea() {
    byte[] bytes1 = BinaryParser.bytesToHex("bytes1".getBytes(StandardCharsets.UTF_8));
    byte[] bytes2 = BinaryParser.bytesToHex("bytes2".getBytes(StandardCharsets.UTF_8));
    assertEquals(
        Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")),
        new ArrayParser(
                ("{\"\\"
                        + new String(bytes1, StandardCharsets.UTF_8)
                        + "\",null,\"\\"
                        + new String(bytes2, StandardCharsets.UTF_8)
                        + "\"}")
                    .getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.bytes(),
                Oid.BYTEA)
            .item);
  }

  @Test
  public void testFloat32() {
    assertEquals(
        Arrays.asList(3.14f, null, -99.99f),
        new ArrayParser(
                "{3.14,null,-99.99}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.float32(),
                Oid.FLOAT4)
            .item);
  }

  @Test
  public void testFloat64() {
    assertEquals(
        Arrays.asList(3.14d, null, -99.99),
        new ArrayParser(
                "{3.14,null,-99.99}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.float64(),
                Oid.FLOAT8)
            .item);
  }

  @Test
  public void testTimestamp() {
    assertEquals(
        Arrays.asList(
            Timestamp.parseTimestamp("2023-02-13T19:15:00.123456Z"),
            null,
            Timestamp.parseTimestamp("2000-01-01T00:00:00Z")),
        new ArrayParser(
                "{\"2023-02-13T19:15:00.123456+00:00\",null,\"2000-01-01T00:00:00+00\"}"
                    .getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.timestamp(),
                Oid.TIMESTAMPTZ)
            .item);
    assertEquals(
        Arrays.asList(
            Timestamp.parseTimestamp("2023-02-13T19:15:00.123456Z"),
            null,
            Timestamp.parseTimestamp("2000-01-01T00:00:00Z")),
        new ArrayParser(
                "{2023-02-13T19:15:00.123456+00:00,null,2000-01-01T00:00:00+00}"
                    .getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.timestamp(),
                Oid.TIMESTAMPTZ)
            .item);
  }

  @Test
  public void testDate() {
    assertEquals(
        Arrays.asList(Date.parseDate("2023-02-13"), null, Date.parseDate("2000-01-01")),
        new ArrayParser(
                "{\"2023-02-13\",null,\"2000-01-01\"}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.date(),
                Oid.DATE)
            .item);
    assertEquals(
        Arrays.asList(Date.parseDate("2023-02-13"), null, Date.parseDate("2000-01-01")),
        new ArrayParser(
                "{2023-02-13,null,2000-01-01}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.date(),
                Oid.DATE)
            .item);
  }

  @Test
  public void testNumeric() {
    assertEquals(
        "{3.14,NULL,6.626}",
        new ArrayParser(
                "{3.14,null,6.626}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.pgNumeric(),
                Oid.NUMERIC)
            .stringParse());
    assertEquals(
        "{3.14,NULL,6.626}",
        new ArrayParser(
                "{\"3.14\",null,\"6.626\"}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.pgNumeric(),
                Oid.NUMERIC)
            .stringParse());
  }

  @Test
  public void testJsonb() {
    String value =
        "{\"{\\\"key1\\\": \\\"value1\\\", \\\"key2\\\": \\\"value2\\\"}\",NULL,\"{\\\"key1\\\": \\\"value3\\\", \\\"key2\\\": \\\"value4\\\"}\"}";
    assertEquals(
        Arrays.asList(
            "{\"key1\": \"value1\", \"key2\": \"value2\"}",
            null,
            "{\"key1\": \"value3\", \"key2\": \"value4\"}"),
        new ArrayParser(
                value.getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.pgJsonb(),
                Oid.JSONB)
            .item);
  }

  @Test
  public void testInvalidTextValue() {
    PGException exception =
        assertThrows(
            PGException.class,
            () ->
                new ArrayParser(
                    "1,2,3".getBytes(StandardCharsets.UTF_8),
                    FormatCode.TEXT,
                    mock(SessionState.class),
                    Type.int64(),
                    Oid.INT8));
    assertEquals("Missing '{' at start of array value: 1,2,3", exception.getMessage());
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());

    exception =
        assertThrows(
            PGException.class,
            () ->
                new ArrayParser(
                    "{,}".getBytes(StandardCharsets.UTF_8),
                    FormatCode.TEXT,
                    mock(SessionState.class),
                    Type.int64(),
                    Oid.INT8));
    assertEquals("Invalid element in array: {,}", exception.getMessage());
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());

    exception =
        assertThrows(
            PGException.class,
            () ->
                new ArrayParser(
                    "{1,}".getBytes(StandardCharsets.UTF_8),
                    FormatCode.TEXT,
                    mock(SessionState.class),
                    Type.int64(),
                    Oid.INT8));
    assertEquals("Invalid element in array: {1,}", exception.getMessage());
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
  }

  @Test
  public void testLinefeed() {
    assertEquals(
        "{1,2,3}",
        new ArrayParser(
                "{1\n,2\n,3\n}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
    assertEquals(
        "{1,2,3}",
        new ArrayParser(
                "{1,\n2,\n3\n}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
    assertEquals(
        "{1,2,3}",
        new ArrayParser(
                "{1,\n\n2\n,\n\n3}".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class),
                Type.int64(),
                Oid.INT8)
            .stringParse());
  }

  @Test
  public void testInt64StringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(Type.int64(), Value.int64Array(Arrays.asList(1L, null, 2L))),
            0,
            mock(SessionState.class));

    assertEquals("{1,NULL,2}", parser.stringParse());
  }

  @Test
  public void testBoolStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(Type.bool(), Value.boolArray(Arrays.asList(true, null, false))),
            0,
            mock(SessionState.class));

    assertEquals("{t,NULL,f}", parser.stringParse());
  }

  @Test
  public void testBytesStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.bytes(),
                Value.bytesArray(
                    Arrays.asList(ByteArray.copyFrom("test1"), null, ByteArray.copyFrom("test2")))),
            0,
            mock(SessionState.class));

    assertEquals("{\"\\\\x7465737431\",NULL,\"\\\\x7465737432\"}", parser.stringParse());
  }

  @Test
  public void testFloat64StringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.float64(), Value.float64Array(Arrays.asList(3.14, null, 6.626))),
            0,
            mock(SessionState.class));

    assertEquals("{3.14,NULL,6.626}", parser.stringParse());
  }

  @Test
  public void testNumericStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.pgNumeric(), Value.pgNumericArray(Arrays.asList("3.14", null, "6.626"))),
            0,
            mock(SessionState.class));

    assertEquals("{3.14,NULL,6.626}", parser.stringParse());
  }

  @Test
  public void testDateStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.date(),
                Value.dateArray(
                    Arrays.asList(
                        Date.parseDate("2022-07-08"), null, Date.parseDate("2000-01-01")))),
            0,
            mock(SessionState.class));

    assertEquals("{\"2022-07-08\",NULL,\"2000-01-01\"}", parser.stringParse());
  }

  @Test
  public void testTimestampStringParse() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));

    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.timestamp(),
                Value.timestampArray(
                    Arrays.asList(
                        Timestamp.parseTimestamp("2022-07-08T07:00:02.123456789Z"),
                        null,
                        Timestamp.parseTimestamp("2000-01-01T00:00:00Z")))),
            0,
            sessionState);

    assertEquals(
        "{\"2022-07-08 07:00:02.123456+00\",NULL,\"2000-01-01 00:00:00+00\"}",
        parser.stringParse());
  }

  @Test
  public void testStringStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.string(), Value.stringArray(Arrays.asList("test1", null, "test2"))),
            0,
            mock(SessionState.class));

    assertEquals("{\"test1\",NULL,\"test2\"}", parser.stringParse());
  }

  @Test
  public void testJsonStringParse() {
    ArrayParser parser =
        new ArrayParser(
            createArrayResultSet(
                Type.pgJsonb(),
                Value.pgJsonbArray(
                    Arrays.asList("{\"key\": \"value1\"}", null, "{\"key\": \"value2\"}"))),
            0,
            mock(SessionState.class));

    assertEquals(
        "{\"{\\\"key\\\": \\\"value1\\\"}\",NULL,\"{\\\"key\\\": \\\"value2\\\"}\"}",
        parser.stringParse());
  }

  @Test
  public void testNullBinaryArray() {
    assertNull(ArrayParser.binaryArrayToList(null, false));
    assertNull(ArrayParser.binaryArrayToList(null, true));
  }

  @Test
  public void testNullTextArray() {
    assertNull(
        ArrayParser.stringArrayToList(null, Oid.UNSPECIFIED, mock(SessionState.class), false));
    assertNull(
        ArrayParser.stringArrayToList(null, Oid.UNSPECIFIED, mock(SessionState.class), true));
    assertNull(
        ArrayParser.stringArrayToList(null, Oid.UNSPECIFIED, mock(SessionState.class), false));
    assertNull(
        ArrayParser.stringArrayToList(null, Oid.UNSPECIFIED, mock(SessionState.class), true));
  }

  static ResultSet createArrayResultSet(Type arrayElementType, Value value) {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("col_array", Type.array(arrayElementType))),
            ImmutableList.of(Struct.newBuilder().set("col_array").to(value).build()));
    resultSet.next();
    return resultSet;
  }
}
