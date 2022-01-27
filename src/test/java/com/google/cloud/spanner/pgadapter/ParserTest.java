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

package com.google.cloud.spanner.pgadapter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.parsers.ArrayParser;
import com.google.cloud.spanner.pgadapter.parsers.BinaryParser;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.DateParser;
import com.google.cloud.spanner.pgadapter.parsers.DoubleParser;
import com.google.cloud.spanner.pgadapter.parsers.IntegerParser;
import com.google.cloud.spanner.pgadapter.parsers.LongParser;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.parsers.StringParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.postgresql.util.ByteConverter;

/**
 * Testing for data format parsing; specifically anything that inherits from the {@link Parser}
 * Class.
 */
@RunWith(JUnit4.class)
public class ParserTest {

  private void validate(
      Parser parser, byte[] byteResult, byte[] stringResult, byte[] spannerResult) {

    // 1. Parse to Binary
    assertThat(parser.parse(DataFormat.POSTGRESQL_BINARY), is(equalTo(byteResult)));
    // 2. Parse to SpannerBinary
    assertThat(parser.parse(DataFormat.POSTGRESQL_TEXT), is(equalTo(stringResult)));
    // 3. Parse to StringBinary
    assertThat(parser.parse(DataFormat.SPANNER), is(equalTo(spannerResult)));
  }

  @Test
  public void testPositiveLongParsing() {
    long value = 1234567890L;
    byte[] byteResult = {0, 0, 0, 0, 73, -106, 2, -46};
    byte[] stringResult = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    Parser parsedValue = new LongParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testNegativeLongParsing() {
    long value = -1234567890L;
    byte[] byteResult = {-1, -1, -1, -1, -74, 105, -3, 46};
    byte[] stringResult = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    Parser parsedValue = new LongParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testPositiveIntegerParsing() {
    int value = 1234567890;
    byte[] byteResult = {73, -106, 2, -46};
    byte[] stringResult = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    Parser parsedValue = new IntegerParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testNegativeIntegerParsing() {
    int value = -1234567890;
    byte[] byteResult = {-74, 105, -3, 46};
    byte[] stringResult = {'-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    Parser parsedValue = new IntegerParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testPositiveDoubleParsing() {
    double value = 1234.56789d;
    byte[] byteResult = {64, -109, 74, 69, -124, -12, -58, -25};
    byte[] stringResult = {'1', '2', '3', '4', '.', '5', '6', '7', '8', '9'};

    Parser parsedValue = new DoubleParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testNegativeDoubleParsing() {
    double value = -1234.56789d;
    byte[] byteResult = {-64, -109, 74, 69, -124, -12, -58, -25};
    byte[] stringResult = {'-', '1', '2', '3', '4', '.', '5', '6', '7', '8', '9'};

    Parser parsedValue = new DoubleParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
  }

  @Test
  public void testFalseBooleanParsing() {
    boolean value = false;
    byte[] stringResult = {'f'};
    byte[] spannerResult = {'f', 'a', 'l', 's', 'e'};

    Parser parsedValue = new BooleanParser(value);

    validate(parsedValue, stringResult, stringResult, spannerResult);
  }

  @Test
  public void testTrueBooleanParsing() {
    boolean value = true;
    byte[] stringResult = {'t'};
    byte[] spannerResult = {'t', 'r', 'u', 'e'};

    Parser parsedValue = new BooleanParser(value);

    validate(parsedValue, stringResult, stringResult, spannerResult);
  }

  @Test
  public void testDateParsing() {
    Date value = new Date(904910400000L); // Google founding date :)

    byte[] byteResult = {-1, -1, -2, 28};
    byte[] stringResult = {'1', '9', '9', '8', '-', '0', '9', '-', '0', '4'};

    Parser parsedValue = new DateParser(value);

    validate(parsedValue, byteResult, stringResult, stringResult);
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

    Parser parsedValue = new StringParser(value);

    validate(parsedValue, stringResult, stringResult, stringResult);
  }

  @Test
  public void testTimestampParsingBytePart() {
    Timestamp value = new Timestamp(904910400000L);

    byte[] byteResult = {-1, -1, -38, 1, -93, -70, 48, 0};

    Parser parsedValue = new TimestampParser(value);

    assertThat(parsedValue.parse(DataFormat.POSTGRESQL_BINARY), is(equalTo(byteResult)));
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
    byte[] value = {(byte) 0b01010101, (byte) 0b10101010};
    byte[] byteResult = {(byte) 0b01010101, (byte) 0b10101010};
    byte[] stringResult = {'U', '\\', '2', '5', '2'};

    Parser parsedValue = new BinaryParser(value);

    validate(parsedValue, byteResult, stringResult, byteResult);
  }

  @Test
  public void testStringArrayParsing() throws SQLException {
    String[] value = {"abc", "def", "jhi"};
    byte[] byteResult = {
      0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -9, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99, 0, 0,
      0, 3, 100, 101, 102, 0, 0, 0, 3, 106, 104, 105
    };
    byte[] stringResult = {
      '{', '"', 'a', 'b', 'c', '"', ',', '"', 'd', 'e', 'f', '"', ',', '"', 'j', 'h', 'i', '"', '}'
    };
    byte[] spannerResult = {
      '[', '"', 'a', 'b', 'c', '"', ',', '"', 'd', 'e', 'f', '"', ',', '"', 'j', 'h', 'i', '"', ']'
    };

    Array sqlArray = Mockito.mock(Array.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(sqlArray.getBaseType()).thenReturn(Types.NVARCHAR);
    Mockito.when(sqlArray.getArray()).thenReturn(value);
    Mockito.when(resultSet.getArray(0)).thenReturn(sqlArray);

    Parser parser = new ArrayParser(resultSet, 0);

    validate(parser, byteResult, stringResult, spannerResult);
  }

  @Test
  public void testLongArrayParsing() throws SQLException {
    Long[] value = {1L, 2L, 3L};
    byte[] byteResult = {
      0, 0, 0, 1, 0, 0, 0, 1, -1, -1, -1, -5, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0,
      0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3
    };
    byte[] stringResult = {'{', '1', ',', '2', ',', '3', '}'};
    byte[] spannerResult = {'[', '1', ',', '2', ',', '3', ']'};

    Array sqlArray = Mockito.mock(Array.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(sqlArray.getBaseType()).thenReturn(Types.BIGINT);
    Mockito.when(sqlArray.getArray()).thenReturn(value);
    Mockito.when(resultSet.getArray(0)).thenReturn(sqlArray);

    Parser parser = new ArrayParser(resultSet, 0);

    validate(parser, byteResult, stringResult, spannerResult);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayArrayParsingFails() throws SQLException {
    Long[] value = {1L, 2L, 3L};

    Array sqlArray = Mockito.mock(Array.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(sqlArray.getBaseType()).thenReturn(Types.ARRAY);
    Mockito.when(sqlArray.getArray()).thenReturn(value);
    Mockito.when(resultSet.getArray(0)).thenReturn(sqlArray);

    new ArrayParser(resultSet, 0);
  }

  @Test
  public void testNumericParsing() throws SQLException {
    String value = "1234567890.1234567890";

    byte[] stringResult = {
      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.', '1', '2', '3', '4', '5', '6', '7', '8',
      '9', '0'
    };

    Parser parser = new StringParser(value);

    validate(parser, stringResult, stringResult, stringResult);
    assertThat(parser.getItem(), is(equalTo(value)));
  }

  @Test
  public void testNumericParsingNaN() throws SQLException {
    String value = "NaN";

    byte[] stringResult = {'N', 'a', 'N'};

    Parser parser = new StringParser(value);

    validate(parser, stringResult, stringResult, stringResult);
    assertThat(parser.getItem(), is(equalTo(value)));
  }
}
