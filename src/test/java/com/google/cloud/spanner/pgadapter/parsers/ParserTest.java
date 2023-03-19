// Copyright 2020 Google LLC
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

import static com.google.cloud.spanner.pgadapter.parsers.Parser.getArrayElementOid;
import static com.google.cloud.spanner.pgadapter.parsers.Parser.toOid;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.spanner.v1.TypeCode;
import java.io.EOFException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

/**
 * Testing for data format parsing; specifically anything that inherits from the {@link Parser}
 * Class.
 */
@RunWith(JUnit4.class)
public class ParserTest {

  private void validate(
      Parser<?> parser, byte[] byteResult, byte[] stringResult, byte[] spannerResult) {

    // 1. Parse to Binary
    assertArrayEquals(byteResult, parser.parse(DataFormat.POSTGRESQL_BINARY));
    // 2. Parse to SpannerBinary
    assertArrayEquals(stringResult, parser.parse(DataFormat.POSTGRESQL_TEXT));
    // 3. Parse to StringBinary
    assertArrayEquals(spannerResult, parser.parse(DataFormat.SPANNER));
  }

  private void validateCreateBinary(byte[] item, int oid, Object value) {
    Parser<?> binary = Parser.create(mock(SessionState.class), item, oid, FormatCode.BINARY);

    assertParserValueEqual(binary, value);
  }

  private void validateCreateText(byte[] item, int oid, Object value) {
    Parser<?> text = Parser.create(mock(SessionState.class), item, oid, FormatCode.TEXT);

    assertParserValueEqual(text, value);
  }

  private void assertParserValueEqual(Parser<?> parser, Object value) {
    if (value instanceof byte[]) {
      assertArrayEquals((byte[]) value, (byte[]) parser.getItem());
    } else if (value instanceof Date) {
      // To prevent false failures because dates are automatically appended with the local timezone.
      assertEquals(value.toString(), parser.getItem().toString());
    } else {
      assertEquals(value, parser.getItem());
    }
  }

  @Test
  public void testPositiveLongParsing() {
    long value = 1234567890L;
    byte[] byteResult = {0, 0, 0, 0, 73, -106, 2, -46};
    byte[] stringResult = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    LongParser parsedValue = new LongParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.INT8, value);
    validateCreateText(stringResult, Oid.INT8, value);
  }

  @Test
  public void testNegativeLongParsing() {
    long value = -1234567890L;
    byte[] byteResult = {-1, -1, -1, -1, -74, 105, -3, 46};
    byte[] stringResult = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    LongParser parsedValue = new LongParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.INT8, value);
    validateCreateText(stringResult, Oid.INT8, value);
  }

  @Test
  public void testPositiveIntegerParsing() {
    int value = 1234567890;
    byte[] byteResult = {73, -106, 2, -46};
    byte[] stringResult = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    IntegerParser parsedValue = new IntegerParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.INT4, value);
    validateCreateText(stringResult, Oid.INT4, value);
  }

  @Test
  public void testNegativeIntegerParsing() {
    int value = -1234567890;
    byte[] byteResult = {-74, 105, -3, 46};
    byte[] stringResult = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    IntegerParser parsedValue = new IntegerParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.INT4, value);
    validateCreateText(stringResult, Oid.INT4, value);
  }

  @Test
  public void testPositiveDoubleParsing() {
    double value = 1234.56789d;
    byte[] byteResult = {64, -109, 74, 69, -124, -12, -58, -25};
    byte[] stringResult = {'1', '2', '3', '4', '.', '5', '6', '7', '8', '9'};

    DoubleParser parsedValue = new DoubleParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.FLOAT8, value);
    validateCreateText(stringResult, Oid.FLOAT8, value);
  }

  @Test
  public void testNegativeDoubleParsing() {
    double value = -1234.56789d;
    byte[] byteResult = {-64, -109, 74, 69, -124, -12, -58, -25};
    byte[] stringResult = {'-', '1', '2', '3', '4', '.', '5', '6', '7', '8', '9'};

    DoubleParser parsedValue = new DoubleParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.FLOAT8, value);
    validateCreateText(stringResult, Oid.FLOAT8, value);
  }

  @Test
  public void testFalseBooleanParsing() {
    boolean value = false;
    byte[] byteResult = {0};
    byte[] stringResult = {'f'};
    byte[] spannerResult = {'f', 'a', 'l', 's', 'e'};

    BooleanParser parsedValue = new BooleanParser(value);

    validate(parsedValue, byteResult, stringResult, spannerResult);
    validateCreateBinary(byteResult, Oid.BIT, value);
    validateCreateText(stringResult, Oid.BIT, value);
  }

  @Test
  public void testTrueBooleanParsing() {
    boolean value = true;
    byte[] byteResult = {1};
    byte[] stringResult = {'t'};
    byte[] spannerResult = {'t', 'r', 'u', 'e'};

    BooleanParser parsedValue = new BooleanParser(value);

    validate(parsedValue, byteResult, stringResult, spannerResult);
    validateCreateBinary(byteResult, Oid.BIT, value);
    validateCreateText(stringResult, Oid.BIT, value);
  }

  @Test
  public void testDateParsing() {
    Date value = Date.fromYearMonthDay(1998, 9, 4); // Google founding date :)

    byte[] byteResult = {-1, -1, -2, 28};
    byte[] stringResult = {'1', '9', '9', '8', '-', '0', '9', '-', '0', '4'};

    DateParser parsedValue = new DateParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
    validateCreateBinary(byteResult, Oid.DATE, value);
    validateCreateText(stringResult, Oid.DATE, value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDateParsingRejectsInvalidDateTooLong() {
    byte[] result = new byte[4];
    ByteConverter.int4(result, 0, Integer.MAX_VALUE);
    new DateParser(result, FormatCode.BINARY);
  }

  @Test
  public void testStringParsing() {
    String value = "This is a String.";

    byte[] stringResult = {
      'T', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', 'S', 't', 'r', 'i', 'n', 'g', '.'
    };

    StringParser parsedValue = new StringParser(value);

    validate(parsedValue, stringResult, stringResult, stringResult);
    validateCreateBinary(stringResult, Oid.VARCHAR, value);
    validateCreateText(stringResult, Oid.VARCHAR, value);
  }

  @Test
  public void testTimestampParsingBytePart() {
    Timestamp value = Timestamp.ofTimeMicroseconds(904910400000000L);

    byte[] byteResult = {-1, -1, -38, 1, -93, -70, 48, 0};

    TimestampParser parsedValue = new TimestampParser(value, mock(SessionState.class));

    assertArrayEquals(byteResult, parsedValue.parse(DataFormat.POSTGRESQL_BINARY));
    validateCreateBinary(byteResult, Oid.TIMESTAMP, value);
  }

  @Test
  public void testBinaryParsing() {
    ByteArray value = ByteArray.copyFrom(new byte[] {(byte) 0b01010101, (byte) 0b10101010});
    byte[] byteResult = {(byte) 0b01010101, (byte) 0b10101010};
    byte[] stringResult = {'\\', 'x', '5', '5', 'a', 'a'};

    BinaryParser parsedValue = new BinaryParser(value);

    validate(parsedValue, byteResult, stringResult, byteResult);
    validateCreateBinary(byteResult, Oid.BYTEA, value);
    validateCreateText(stringResult, Oid.BYTEA, value);
  }

  @Test
  public void testStringArrayParsing() {
    String[] value = {"abc", "def", "jhi"};
    // The binary format of a PG array should contain the OID of the element type and not the array
    // OID. For VARCHAR that means 1043 and not 1015
    // See
    // https://github.com/pgjdbc/pgjdbc/blob/60a81034fd003551fc863033f491b2d0ed1dfa80/pgjdbc/src/main/java/org/postgresql/jdbc/ArrayDecoding.java#L505
    // We can test this more thoroughly when
    // https://github.com/GoogleCloudPlatform/pgadapter/pull/36
    // has been merged.
    byte[] byteResult = {
      0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 4, 19, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0,
      3, 100, 101, 102, 0, 0, 0, 3, 106, 104, 105
    };
    byte[] stringResult = {
      '{', '"', 'a', 'b', 'c', '"', ',', '"', 'd', 'e', 'f', '"', ',', '"', 'j', 'h', 'i', '"', '}'
    };
    byte[] spannerResult = {
      '[', '"', 'a', 'b', 'c', '"', ',', '"', 'd', 'e', 'f', '"', ',', '"', 'j', 'h', 'i', '"', ']'
    };

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.string()));
    when(resultSet.getValue(0)).thenReturn(Value.stringArray(Arrays.asList(value)));

    ArrayParser parser = new ArrayParser(resultSet, 0, mock(SessionState.class));

    validate(parser, byteResult, stringResult, spannerResult);
  }

  @Test
  public void testLongArrayParsing() {
    Long[] value = {1L, 2L, 3L};
    // The binary format of a PG array should contain the OID of the element type and not the array
    // OID. For INT8 that means 20 and not 1016
    // See
    // https://github.com/pgjdbc/pgjdbc/blob/60a81034fd003551fc863033f491b2d0ed1dfa80/pgjdbc/src/main/java/org/postgresql/jdbc/ArrayDecoding.java#L505
    // We can test this more thoroughly when
    // https://github.com/GoogleCloudPlatform/pgadapter/pull/36
    // has been merged.
    byte[] byteResult = {
      0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
      1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3
    };
    byte[] stringResult = {'{', '1', ',', '2', ',', '3', '}'};
    byte[] spannerResult = {'[', '1', ',', '2', ',', '3', ']'};

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.int64()));
    when(resultSet.getValue(0)).thenReturn(Value.int64Array(Arrays.asList(value)));

    ArrayParser parser = new ArrayParser(resultSet, 0, mock(SessionState.class));

    validate(parser, byteResult, stringResult, spannerResult);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayArrayParsingFails() {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.array(Type.int64())));

    new ArrayParser(resultSet, 0, mock(SessionState.class));
  }

  @Test
  public void testNumericParsing() {
    String value = "1234567890.1234567890";

    byte[] byteResult = ByteConverter.numeric(new BigDecimal("1234567890.1234567890"));
    byte[] stringResult = {
      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.', '1', '2', '3', '4', '5', '6', '7', '8',
      '9', '0'
    };

    NumericParser parser = new NumericParser(value);

    validate(parser, byteResult, stringResult, stringResult);
    assertEquals(value, parser.getItem());
    validateCreateBinary(byteResult, Oid.NUMERIC, value);
    validateCreateText(stringResult, Oid.NUMERIC, value);
  }

  @Test
  public void testNumericParsingNull() {
    NumericParser parser = new NumericParser(null);

    assertNull(parser.stringParse());
    assertNull(parser.binaryParse());
  }

  @Test
  public void testNumericParsingInvalid() {
    NumericParser parser = new NumericParser("invalid-number");

    // String parsing will work, and will just propagate the nonsense number to the frontend.
    assertEquals("invalid-number", parser.stringParse());

    // Binary parsing fails, as it tries to convert it to a number, so it can be encoded in a byte
    // array.
    SpannerException binaryException = assertThrows(SpannerException.class, parser::binaryParse);
    assertEquals(ErrorCode.INVALID_ARGUMENT, binaryException.getErrorCode());
  }

  @Test
  public void testNumericParsingNaN() {
    String value = "NaN";
    byte[] byteResult = new byte[8];
    ByteConverter.int2(byteResult, 4, (short) 0xC000);

    byte[] stringResult = {'N', 'a', 'N'};

    NumericParser parser = new NumericParser(value);

    validate(parser, byteResult, stringResult, stringResult);
    assertEquals(value, parser.getItem());
    validateCreateText(stringResult, Oid.NUMERIC, value);
  }

  @Test
  public void testTypeToOid() {
    assertEquals(Oid.INT8, toOid(createType(TypeCode.INT64)));
    assertEquals(Oid.BOOL, toOid(createType(TypeCode.BOOL)));
    assertEquals(Oid.VARCHAR, toOid(createType(TypeCode.STRING)));
    assertEquals(Oid.JSONB, toOid(createType(TypeCode.JSON)));
    assertEquals(Oid.FLOAT8, toOid(createType(TypeCode.FLOAT64)));
    assertEquals(Oid.TIMESTAMPTZ, toOid(createType(TypeCode.TIMESTAMP)));
    assertEquals(Oid.DATE, toOid(createType(TypeCode.DATE)));
    assertEquals(Oid.NUMERIC, toOid(createType(TypeCode.NUMERIC)));
    assertEquals(Oid.BYTEA, toOid(createType(TypeCode.BYTES)));

    assertEquals(Oid.INT8_ARRAY, toOid(createArrayType(TypeCode.INT64)));
    assertEquals(Oid.BOOL_ARRAY, toOid(createArrayType(TypeCode.BOOL)));
    assertEquals(Oid.VARCHAR_ARRAY, toOid(createArrayType(TypeCode.STRING)));
    assertEquals(Oid.JSONB_ARRAY, toOid(createArrayType(TypeCode.JSON)));
    assertEquals(Oid.FLOAT8_ARRAY, toOid(createArrayType(TypeCode.FLOAT64)));
    assertEquals(Oid.TIMESTAMPTZ_ARRAY, toOid(createArrayType(TypeCode.TIMESTAMP)));
    assertEquals(Oid.DATE_ARRAY, toOid(createArrayType(TypeCode.DATE)));
    assertEquals(Oid.NUMERIC_ARRAY, toOid(createArrayType(TypeCode.NUMERIC)));
    assertEquals(Oid.BYTEA_ARRAY, toOid(createArrayType(TypeCode.BYTES)));

    assertThrows(PGException.class, () -> toOid(createType(TypeCode.STRUCT)));
    assertThrows(PGException.class, () -> toOid(createArrayType(TypeCode.ARRAY)));
    assertThrows(PGException.class, () -> toOid(createArrayType(TypeCode.STRUCT)));
  }

  @Test
  public void testParseInt8Array() {
    assertEquals(
        Arrays.asList(1L, null, 2L),
        Parser.create(
                mock(SessionState.class),
                "{1,null,2}".getBytes(StandardCharsets.UTF_8),
                Oid.INT8_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseInt4Array() {
    assertEquals(
        Arrays.asList(1, null, 2),
        Parser.create(
                mock(SessionState.class),
                "{1,null,2}".getBytes(StandardCharsets.UTF_8),
                Oid.INT4_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseInt2Array() {
    assertEquals(
        Arrays.asList((short) 1, null, (short) 2),
        Parser.create(
                mock(SessionState.class),
                "{1,null,2}".getBytes(StandardCharsets.UTF_8),
                Oid.INT2_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseBoolArray() {
    assertEquals(
        Arrays.asList(true, null, false),
        Parser.create(
                mock(SessionState.class),
                "{t,null,f}".getBytes(StandardCharsets.UTF_8),
                Oid.BOOL_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseFloat8Array() {
    assertEquals(
        Arrays.asList(3.14d, null, -99.99d),
        Parser.create(
                mock(SessionState.class),
                "{3.14,null,-99.99}".getBytes(StandardCharsets.UTF_8),
                Oid.FLOAT8_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseFloat4Array() {
    assertEquals(
        Arrays.asList(3.14f, null, -99.99f),
        Parser.create(
                mock(SessionState.class),
                "{3.14,null,-99.99}".getBytes(StandardCharsets.UTF_8),
                Oid.FLOAT4_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseNumericArray() {
    assertEquals(
        Arrays.asList("3.14", null, "-99.99"),
        Parser.create(
                mock(SessionState.class),
                "{3.14,null,-99.99}".getBytes(StandardCharsets.UTF_8),
                Oid.NUMERIC_ARRAY,
                FormatCode.TEXT)
            .getItem());
    assertEquals(
        Arrays.asList("3.14", null, "-99.99"),
        Parser.create(
                mock(SessionState.class),
                "{\"3.14\",null,\"-99.99\"}".getBytes(StandardCharsets.UTF_8),
                Oid.NUMERIC_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseByteaArray() {
    assertEquals(
        Arrays.asList(ByteArray.copyFrom("bytes1"), null, ByteArray.copyFrom("bytes2")),
        Parser.create(
                mock(SessionState.class),
                ("{\"\\"
                        + new String(
                            BinaryParser.bytesToHex("bytes1".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8)
                        + "\",null,\"\\"
                        + new String(
                            BinaryParser.bytesToHex("bytes2".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8)
                        + "\"}")
                    .getBytes(StandardCharsets.UTF_8),
                Oid.BYTEA_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseVarcharArray() {
    assertEquals(
        Arrays.asList("string1", null, "string2"),
        Parser.create(
                mock(SessionState.class),
                "{\"string1\",null,\"string2\"}".getBytes(StandardCharsets.UTF_8),
                Oid.VARCHAR_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseTextArray() {
    assertEquals(
        Arrays.asList("string1", null, "string2"),
        Parser.create(
                mock(SessionState.class),
                "{\"string1\",null,\"string2\"}".getBytes(StandardCharsets.UTF_8),
                Oid.TEXT_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseUuidArray() {
    assertEquals(
        Arrays.asList(
            "1e4d8cd3-099d-4954-a5c1-85272713c3e9", null, "5fab5fcf-3c39-42cf-b41e-b40f1a9c3bdd"),
        Parser.create(
                mock(SessionState.class),
                "{\"1e4d8cd3-099d-4954-a5c1-85272713c3e9\",null,\"5fab5fcf-3c39-42cf-b41e-b40f1a9c3bdd\"}"
                    .getBytes(StandardCharsets.UTF_8),
                Oid.TEXT_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseDateArray() {
    assertEquals(
        Arrays.asList(Date.parseDate("2000-01-01"), null, Date.parseDate("1970-01-01")),
        Parser.create(
                mock(SessionState.class),
                "{\"2000-01-01\",null,\"1970-01-01\"}".getBytes(StandardCharsets.UTF_8),
                Oid.DATE_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseTimestamptzArray() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("Europe/Amsterdam"));
    assertEquals(
        Arrays.asList(
            Timestamp.parseTimestamp("2023-02-14T13:38:00.123456Z"),
            null,
            Timestamp.parseTimestamp("2000-01-01T00:00:00Z")),
        Parser.create(
                sessionState,
                "{\"2023-02-14 14:38:00.123456\",null,\"2000-01-01 00:00:00+00\"}"
                    .getBytes(StandardCharsets.UTF_8),
                Oid.TIMESTAMPTZ_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseTimestampArray() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("America/Los_Angeles"));
    assertEquals(
        Arrays.asList(
            Timestamp.parseTimestamp("2023-02-15T02:38:00.123456000Z"),
            null,
            Timestamp.parseTimestamp("2000-01-01T00:00:00Z")),
        Parser.create(
                sessionState,
                "{\"2023-02-14 18:38:00.123456\",null,\"2000-01-01 00:00:00+00\"}"
                    .getBytes(StandardCharsets.UTF_8),
                Oid.TIMESTAMP_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testParseJsonbArray() {
    assertEquals(
        Arrays.asList("{\"key\": \"value\"}", null, "{\"key\": [0, 1]}"),
        Parser.create(
                mock(SessionState.class),
                "{\"{\\\"key\\\": \\\"value\\\"}\", null, \"{\\\"key\\\": [0, 1]}\"}"
                    .getBytes(StandardCharsets.UTF_8),
                Oid.TEXT_ARRAY,
                FormatCode.TEXT)
            .getItem());
  }

  @Test
  public void testToType() {
    assertEquals(Type.int64(), Parser.toType(Oid.INT2));
    assertEquals(Type.int64(), Parser.toType(Oid.INT4));
    assertEquals(Type.int64(), Parser.toType(Oid.INT8));
    assertEquals(Type.bool(), Parser.toType(Oid.BOOL));
    assertEquals(Type.bytes(), Parser.toType(Oid.BYTEA));
    assertEquals(Type.date(), Parser.toType(Oid.DATE));
    assertEquals(Type.timestamp(), Parser.toType(Oid.TIMESTAMP));
    assertEquals(Type.timestamp(), Parser.toType(Oid.TIMESTAMPTZ));
    assertEquals(Type.float64(), Parser.toType(Oid.FLOAT4));
    assertEquals(Type.float64(), Parser.toType(Oid.FLOAT8));
    assertEquals(Type.pgNumeric(), Parser.toType(Oid.NUMERIC));
    assertEquals(Type.string(), Parser.toType(Oid.VARCHAR));
    assertEquals(Type.string(), Parser.toType(Oid.TEXT));
    assertEquals(Type.string(), Parser.toType(Oid.UUID));
    assertEquals(Type.pgJsonb(), Parser.toType(Oid.JSONB));

    assertEquals(Type.array(Type.int64()), Parser.toType(Oid.INT2_ARRAY));
    assertEquals(Type.array(Type.int64()), Parser.toType(Oid.INT4_ARRAY));
    assertEquals(Type.array(Type.int64()), Parser.toType(Oid.INT8_ARRAY));
    assertEquals(Type.array(Type.bool()), Parser.toType(Oid.BOOL_ARRAY));
    assertEquals(Type.array(Type.bytes()), Parser.toType(Oid.BYTEA_ARRAY));
    assertEquals(Type.array(Type.date()), Parser.toType(Oid.DATE_ARRAY));
    assertEquals(Type.array(Type.timestamp()), Parser.toType(Oid.TIMESTAMP_ARRAY));
    assertEquals(Type.array(Type.timestamp()), Parser.toType(Oid.TIMESTAMPTZ_ARRAY));
    assertEquals(Type.array(Type.float64()), Parser.toType(Oid.FLOAT4_ARRAY));
    assertEquals(Type.array(Type.float64()), Parser.toType(Oid.FLOAT8_ARRAY));
    assertEquals(Type.array(Type.pgNumeric()), Parser.toType(Oid.NUMERIC_ARRAY));
    assertEquals(Type.array(Type.string()), Parser.toType(Oid.VARCHAR_ARRAY));
    assertEquals(Type.array(Type.string()), Parser.toType(Oid.TEXT_ARRAY));
    assertEquals(Type.array(Type.string()), Parser.toType(Oid.UUID_ARRAY));
    assertEquals(Type.array(Type.pgJsonb()), Parser.toType(Oid.JSONB_ARRAY));

    assertThrows(PGException.class, () -> Parser.toType(-1000));
    assertThrows(PGException.class, () -> Parser.toType(Oid.UNSPECIFIED));
  }

  @Test
  public void testParseMultiDimBinaryArray() {
    byte[] data = new byte[20];
    // 2 dimensions
    ByteConverter.int4(data, 0, 2);
    // Null flag
    ByteConverter.int4(data, 4, 0);
    // oid
    ByteConverter.int4(data, 8, Oid.INT8);
    // length
    ByteConverter.int4(data, 12, 0);
    // base 1
    ByteConverter.int4(data, 16, 1);

    PGException exception =
        assertThrows(
            PGException.class,
            () -> Parser.create(mock(SessionState.class), data, Oid.INT8_ARRAY, FormatCode.BINARY));
    assertEquals("Only single-dimension arrays are supported", exception.getMessage());
  }

  @Test
  public void testParseBinaryArrayWithTooLittleData() {
    byte[] data = new byte[20];
    // 1 dimension
    ByteConverter.int4(data, 0, 1);
    // Null flag
    ByteConverter.int4(data, 4, 0);
    // oid
    ByteConverter.int4(data, 8, Oid.INT8);
    // length = 10, but we don't write any actual data.
    ByteConverter.int4(data, 12, 10);
    // base 1
    ByteConverter.int4(data, 16, 1);

    PGException exception =
        assertThrows(
            PGException.class,
            () -> Parser.create(mock(SessionState.class), data, Oid.INT8_ARRAY, FormatCode.BINARY));
    assertEquals("Invalid array value", exception.getMessage());
    assertEquals(EOFException.class, exception.getCause().getClass());
  }

  @Test
  public void testGetArrayElementOid() {
    assertEquals(Oid.BOOL, getArrayElementOid(Oid.BOOL_ARRAY));
    assertEquals(Oid.BYTEA, getArrayElementOid(Oid.BYTEA_ARRAY));
    assertEquals(Oid.DATE, getArrayElementOid(Oid.DATE_ARRAY));
    assertEquals(Oid.FLOAT4, getArrayElementOid(Oid.FLOAT4_ARRAY));
    assertEquals(Oid.FLOAT8, getArrayElementOid(Oid.FLOAT8_ARRAY));
    assertEquals(Oid.INT2, getArrayElementOid(Oid.INT2_ARRAY));
    assertEquals(Oid.INT4, getArrayElementOid(Oid.INT4_ARRAY));
    assertEquals(Oid.INT8, getArrayElementOid(Oid.INT8_ARRAY));
    assertEquals(Oid.JSONB, getArrayElementOid(Oid.JSONB_ARRAY));
    assertEquals(Oid.NUMERIC, getArrayElementOid(Oid.NUMERIC_ARRAY));
    assertEquals(Oid.VARCHAR, getArrayElementOid(Oid.VARCHAR_ARRAY));
    assertEquals(Oid.TEXT, getArrayElementOid(Oid.TEXT_ARRAY));
    assertEquals(Oid.UUID, getArrayElementOid(Oid.UUID_ARRAY));
    assertEquals(Oid.TIMESTAMP, getArrayElementOid(Oid.TIMESTAMP_ARRAY));
    assertEquals(Oid.TIMESTAMPTZ, getArrayElementOid(Oid.TIMESTAMPTZ_ARRAY));
    assertEquals(Oid.UNSPECIFIED, getArrayElementOid(Integer.MAX_VALUE));
  }

  @Test
  public void testToOid() {
    assertEquals(Oid.BOOL, toOid(Type.bool()));
    assertEquals(Oid.BYTEA, toOid(Type.bytes()));
    assertEquals(Oid.DATE, toOid(Type.date()));
    assertEquals(Oid.FLOAT8, toOid(Type.float64()));
    assertEquals(Oid.INT8, toOid(Type.int64()));
    assertEquals(Oid.JSONB, toOid(Type.pgJsonb()));
    assertEquals(Oid.NUMERIC, toOid(Type.pgNumeric()));
    assertEquals(Oid.VARCHAR, toOid(Type.string()));
    assertEquals(Oid.TIMESTAMPTZ, toOid(Type.timestamp()));

    assertEquals(Oid.BOOL_ARRAY, toOid(Type.array(Type.bool())));
    assertEquals(Oid.BYTEA_ARRAY, toOid(Type.array(Type.bytes())));
    assertEquals(Oid.DATE_ARRAY, toOid(Type.array(Type.date())));
    assertEquals(Oid.FLOAT8_ARRAY, toOid(Type.array(Type.float64())));
    assertEquals(Oid.INT8_ARRAY, toOid(Type.array(Type.int64())));
    assertEquals(Oid.JSONB_ARRAY, toOid(Type.array(Type.pgJsonb())));
    assertEquals(Oid.NUMERIC_ARRAY, toOid(Type.array(Type.pgNumeric())));
    assertEquals(Oid.VARCHAR_ARRAY, toOid(Type.array(Type.string())));
    assertEquals(Oid.TIMESTAMPTZ_ARRAY, toOid(Type.array(Type.timestamp())));

    assertThrows(PGException.class, () -> toOid(Type.array(Type.struct())));
    assertThrows(PGException.class, () -> toOid(Type.struct()));
  }

  static com.google.spanner.v1.Type createType(TypeCode code) {
    return com.google.spanner.v1.Type.newBuilder().setCode(code).build();
  }

  static com.google.spanner.v1.Type createArrayType(TypeCode code) {
    return com.google.spanner.v1.Type.newBuilder()
        .setCode(TypeCode.ARRAY)
        .setArrayElementType(com.google.spanner.v1.Type.newBuilder().setCode(code).build())
        .build();
  }
}
