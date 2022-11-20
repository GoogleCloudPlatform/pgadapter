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

import static com.google.cloud.spanner.pgadapter.parsers.Parser.toOid;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
import com.google.spanner.v1.TypeCode;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
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
    Parser<?> binary = Parser.create(item, oid, FormatCode.BINARY);

    assertParserValueEqual(binary, value);
  }

  private void validateCreateText(byte[] item, int oid, Object value) {
    Parser<?> text = Parser.create(item, oid, FormatCode.TEXT);

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
  public void testDateParsingDateValidityChecks() {
    assertTrue(DateParser.isDate("1998-09-01 00:00"));
    assertTrue(DateParser.isDate("1998-09-01 +00:00"));
    assertTrue(DateParser.isDate("1998-09-01 -00:00"));
    assertFalse(DateParser.isDate("This is not a date right?"));
    assertFalse(DateParser.isDate("1998-09-01 *00:00"));
    assertFalse(DateParser.isDate("1998-a-01 00:00"));
    assertFalse(DateParser.isDate("1998-01-a 00:00"));
    assertFalse(DateParser.isDate("1998-01-01 a:00"));
    assertFalse(DateParser.isDate("1998-01-01 00:a"));
    assertFalse(DateParser.isDate("1998-01 00:00"));
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

    TimestampParser parsedValue = new TimestampParser(value);

    assertArrayEquals(byteResult, parsedValue.parse(DataFormat.POSTGRESQL_BINARY));
    validateCreateBinary(byteResult, Oid.TIMESTAMP, value);
  }

  @Test
  public void testTimestampParsingTimestampValidityChecks() {
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.0"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000Z"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000z"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000+00"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000+00:00"));
    assertTrue(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000-00:00"));
    assertFalse(TimestampParser.isTimestamp("This is not a timestamp right?"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000z00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000z00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000+"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000-"));
    assertFalse(TimestampParser.isTimestamp("99999-09-01 00:00:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("aaaa-09-01 00:00:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-aa-01 00:00:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-aa 00:00:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 aa:00:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:aa:00.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:aa.000000000+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.a+00:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000+aa:00"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000+00:aa"));
    assertFalse(TimestampParser.isTimestamp("1998-09-01 00:00:00.000000000*00:00"));
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

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.string()));
    when(resultSet.getValue(0)).thenReturn(Value.stringArray(Arrays.asList(value)));

    ArrayParser parser = new ArrayParser(resultSet, 0);

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

    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.int64()));
    when(resultSet.getValue(0)).thenReturn(Value.int64Array(Arrays.asList(value)));

    ArrayParser parser = new ArrayParser(resultSet, 0);

    validate(parser, byteResult, stringResult, spannerResult);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayArrayParsingFails() {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.getColumnType(0)).thenReturn(Type.array(Type.array(Type.int64())));

    new ArrayParser(resultSet, 0);
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
