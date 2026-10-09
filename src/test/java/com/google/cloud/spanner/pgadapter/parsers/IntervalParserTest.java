// Copyright 2025 Google LLC
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Interval;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IntervalParserTest {

  @Test
  public void testConvertToPG() throws IOException {
    Interval interval =
        Interval.fromMonthsDaysNanos(
            2, 5, BigInteger.valueOf((3 * 3600 + 4 * 60 + 5) * 1_000_000_000L + 123456000L));
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInterval(0)).thenReturn(interval);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    // Text format
    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] textBytes = IntervalParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT);
    ByteArrayOutputStream expectedText = new ByteArrayOutputStream();
    DataOutputStream expectedTextStream = new DataOutputStream(expectedText);
    expectedTextStream.writeInt(textBytes.length);
    expectedTextStream.write(textBytes);
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    byte[] binaryBytes = IntervalParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY);
    ByteArrayOutputStream expectedBinary = new ByteArrayOutputStream();
    DataOutputStream expectedBinaryStream = new DataOutputStream(expectedBinary);
    expectedBinaryStream.writeInt(16);
    expectedBinaryStream.write(binaryBytes);
    assertArrayEquals(expectedBinary.toByteArray(), output.toByteArray());
    output.reset();

    // Spanner format
    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.SPANNER));
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
  }

  @Test
  public void testBinaryAndTextParse() {
    Interval interval =
        Interval.fromMonthsDaysNanos(
            1, 2, BigInteger.valueOf((10 * 3600 + 20 * 60 + 30) * 1_000_000_000L));
    byte[] binary = IntervalParser.convertToPGBinary(interval);
    Interval parsedFromBinary = IntervalParser.toInterval(binary, FormatCode.BINARY);
    assertEquals(interval, parsedFromBinary);

    IntervalParser parser = new IntervalParser(binary, FormatCode.BINARY);
    assertArrayEquals(binary, parser.binaryParse());

    parser = new IntervalParser(null, FormatCode.BINARY);
    assertNull(parser.binaryParse());
    assertNull(parser.stringParse());
  }

  @Test
  public void testConvertToPGNegativeInterval() throws IOException {
    Interval negativeInterval =
        Interval.fromMonthsDaysNanos(
            -2, -5, BigInteger.valueOf((-3 * 3600 - 4 * 60 - 5) * 1_000_000_000L));
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInterval(0)).thenReturn(negativeInterval);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    // Text format
    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] textBytes = IntervalParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT);
    ByteArrayOutputStream expectedText = new ByteArrayOutputStream();
    DataOutputStream expectedTextStream = new DataOutputStream(expectedText);
    expectedTextStream.writeInt(textBytes.length);
    expectedTextStream.write(textBytes);
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    byte[] binaryBytes = IntervalParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY);
    ByteArrayOutputStream expectedBinary = new ByteArrayOutputStream();
    DataOutputStream expectedBinaryStream = new DataOutputStream(expectedBinary);
    expectedBinaryStream.writeInt(16);
    expectedBinaryStream.write(binaryBytes);
    assertArrayEquals(expectedBinary.toByteArray(), output.toByteArray());

    // Verify round-trip parsing of formatted negative interval
    assertEquals(
        negativeInterval, IntervalParser.toInterval(new String(textBytes, StandardCharsets.UTF_8)));

    // Sub-microsecond negative interval does not produce -00:00:00.000000
    ResultSet subMicroResultSet = mock(ResultSet.class);
    when(subMicroResultSet.getInterval(0))
        .thenReturn(Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-500)));
    byte[] subMicroBytes =
        IntervalParser.convertToPG(subMicroResultSet, 0, DataFormat.POSTGRESQL_TEXT);
    assertEquals("0 mons 0 days 00:00:0.000000", new String(subMicroBytes, StandardCharsets.UTF_8));

    // Binary microseconds overflow throws ArithmeticException
    BigInteger overflowNanos =
        BigInteger.valueOf(Long.MAX_VALUE)
            .multiply(BigInteger.valueOf(1000))
            .add(BigInteger.valueOf(1000));
    Interval overflowInterval = Interval.fromMonthsDaysNanos(0, 0, overflowNanos);
    assertThrows(
        ArithmeticException.class, () -> IntervalParser.convertToPGBinary(overflowInterval));
  }

  @Test
  public void testToIntervalText() {
    // Weeks and ago
    assertEquals(Interval.ofDays(14), IntervalParser.toInterval("2 weeks"));
    assertEquals(Interval.ofDays(14), IntervalParser.toInterval("2 week"));
    assertEquals(Interval.ofDays(14), IntervalParser.toInterval("2 w"));
    assertEquals(Interval.ofMonths(-12), IntervalParser.toInterval("1 year ago"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("@ 1 year 2 mons ago"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("@ 1 year 2 mons"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("1 year 2 months"));

    // Full standard units
    assertEquals(
        Interval.fromMonthsDaysNanos(
            14, 3, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("1 year 2 months 3 days 4 hours 5 minutes 6 seconds"));

    // Fractional seconds
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(50_000_000L)),
        IntervalParser.toInterval("0.05 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(50_000L)),
        IntervalParser.toInterval("0.00005 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(100L)),
        IntervalParser.toInterval("0.0000001 seconds"));

    // Negative intervals
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-1_500_000_000L)),
        IntervalParser.toInterval("-00:00:01.5"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-30_000_000_000L)),
        IntervalParser.toInterval("-30 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-500_000L)),
        IntervalParser.toInterval("-500 microseconds"));

    // Bare numbers
    assertEquals(Interval.ofSeconds(10), IntervalParser.toInterval("10"));
    assertEquals(Interval.ofSeconds(-10), IntervalParser.toInterval("-10"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(10_500_000_000L)),
        IntervalParser.toInterval("10.5"));

    // Fractional units
    assertEquals(Interval.ofMonths(18), IntervalParser.toInterval("1.5 years"));
    assertEquals(
        Interval.fromMonthsDaysNanos(1, 15, BigInteger.ZERO),
        IntervalParser.toInterval("1.5 months"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 10, BigInteger.valueOf(12L * 3600 * 1_000_000_000L)),
        IntervalParser.toInterval("1.5 weeks"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 1, BigInteger.valueOf(12L * 3600 * 1_000_000_000L)),
        IntervalParser.toInterval("1.5 days"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(90L * 60 * 1_000_000_000L)),
        IntervalParser.toInterval("1.5 hours"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(90L * 1_000_000_000L)),
        IntervalParser.toInterval("1.5 minutes"));

    // ISO 8601
    assertEquals(Interval.ofDays(14), IntervalParser.toInterval("P2W"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            14, 3, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("P1Y2M3DT4H5M6S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(50_000_000L)),
        IntervalParser.toInterval("PT0.05S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            14, 3, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("P0001-02-03T04:05:06"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("-P1Y2M"));

    // SQL standard format
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("1-2"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("-1-2"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, 3, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("3 04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, -3, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("-3 04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, -3, BigInteger.valueOf((-4 * 3600L - 5 * 60 - 6) * 1_000_000_000L)),
        IntervalParser.toInterval("-3 -04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, -3, BigInteger.valueOf((-4 * 3600L - 5 * 60 - 6) * 1_000_000_000L)),
        IntervalParser.toInterval("3 04:05:06 ago"));

    // Time string format
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, 0, BigInteger.valueOf((4 * 3600L + 5 * 60 + 6) * 1_000_000_000L)),
        IntervalParser.toInterval("04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0, 0, BigInteger.valueOf((4 * 3600L + 5 * 60) * 1_000_000_000L)),
        IntervalParser.toInterval("04:05"));

    // Large units and sub-second units
    assertEquals(
        Interval.ofMonths((1000 + 200 + 30) * 12),
        IntervalParser.toInterval("1 millennium 2 centuries 3 decades"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(10 * 1_000_000L + 20 * 1_000L)),
        IntervalParser.toInterval("10 ms 20 us"));
  }

  @Test
  public void testToIntervalInvalid() {
    // Duplicate units
    assertInvalidInterval("1 day 5 days");
    assertInvalidInterval("1 year 2 years");
    assertInvalidInterval("2 weeks 1 week");
    assertInvalidInterval("1 hour 2 hours");
    assertInvalidInterval("1-2 3-4");

    // Conflicting or invalid combinations
    assertInvalidInterval("1 hour 02:00:00");
    assertInvalidInterval("01:00:00 02:00:00");
    assertInvalidInterval("1-2-3");
    assertInvalidInterval("1-2-3-4");

    // Invalid syntax or numbers
    assertInvalidInterval("invalid");
    assertInvalidInterval("1 foo");
    assertInvalidInterval("foo 1");
    assertInvalidInterval("");
    assertInvalidInterval("   ");
    assertInvalidInterval(null);
  }

  private void assertInvalidInterval(String value) {
    PGException exception = assertThrows(PGException.class, () -> IntervalParser.toInterval(value));
    assertEquals(SQLState.SyntaxError, exception.getSQLState());
  }

  @Test
  public void testBind() {
    ImmutableMap.Builder<String, Value> parametersBuilder = ImmutableMap.builder();
    IntervalParser.bind(
        parametersBuilder, "p1", "2 weeks".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT);
    assertEquals(
        ImmutableMap.of("p1", Value.interval(Interval.ofDays(14))), parametersBuilder.build());

    parametersBuilder = ImmutableMap.builder();
    Interval interval =
        Interval.fromMonthsDaysNanos(
            1, 2, BigInteger.valueOf((10 * 3600 + 20 * 60 + 30) * 1_000_000_000L));
    byte[] binaryBytes = IntervalParser.convertToPGBinary(interval);
    IntervalParser.bind(parametersBuilder, "p2", binaryBytes, FormatCode.BINARY);
    assertEquals(ImmutableMap.of("p2", Value.interval(interval)), parametersBuilder.build());

    parametersBuilder = ImmutableMap.builder();
    IntervalParser parser = new IntervalParser(Interval.ofDays(14));
    parser.bind(parametersBuilder, "p3");
    assertEquals(
        ImmutableMap.of("p3", Value.interval(Interval.ofDays(14))), parametersBuilder.build());
  }

  @Test
  public void testToPGStringNegativeIntervalFormatting() {
    Interval interval1 = Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-30_000_000_000L));
    ResultSet resultSet1 = mock(ResultSet.class);
    when(resultSet1.getInterval(0)).thenReturn(interval1);
    byte[] textBytes1 = IntervalParser.convertToPG(resultSet1, 0, DataFormat.POSTGRESQL_TEXT);
    assertEquals("0 mons 0 days -00:00:30.000000", new String(textBytes1, StandardCharsets.UTF_8));

    Interval interval2 = Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-500_000L));
    ResultSet resultSet2 = mock(ResultSet.class);
    when(resultSet2.getInterval(0)).thenReturn(interval2);
    byte[] textBytes2 = IntervalParser.convertToPG(resultSet2, 0, DataFormat.POSTGRESQL_TEXT);
    assertEquals("0 mons 0 days -00:00:0.000500", new String(textBytes2, StandardCharsets.UTF_8));

    // Negative minutes with 0 hours
    Interval intervalMinutes =
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-300_000_000_000L));
    ResultSet resultSetMinutes = mock(ResultSet.class);
    when(resultSetMinutes.getInterval(0)).thenReturn(intervalMinutes);
    byte[] textBytesMinutes =
        IntervalParser.convertToPG(resultSetMinutes, 0, DataFormat.POSTGRESQL_TEXT);
    assertEquals(
        "0 mons 0 days -00:05:0.000000", new String(textBytesMinutes, StandardCharsets.UTF_8));
  }

  @Test
  public void testParserMethods() {
    Interval interval = Interval.ofDays(14);
    IntervalParser parser = new IntervalParser(interval);
    assertEquals("0 mons 14 days 00:00:0.000000", parser.stringParse());
    assertEquals("P14D", parser.spannerParse());
    assertArrayEquals(IntervalParser.convertToPGBinary(interval), parser.binaryParse());
    assertEquals(interval, parser.getItem());

    IntervalParser nullParser = new IntervalParser(null);
    assertNull(nullParser.stringParse());
    assertNull(nullParser.spannerParse());
    assertNull(nullParser.binaryParse());
    assertNull(nullParser.getItem());

    IntervalParser textParser =
        new IntervalParser("2 weeks".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT);
    assertEquals(interval, textParser.getItem());

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInterval(0)).thenReturn(interval);
    IntervalParser resultSetParser = new IntervalParser(resultSet, 0);
    assertEquals(interval, resultSetParser.getItem());

    assertThrows(NullPointerException.class, () -> new IntervalParser(new byte[16], null));
  }

  @Test
  public void testConvertToPGFormats() throws IOException {
    Interval interval = Interval.ofDays(3);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getInterval(0)).thenReturn(interval);
    SessionState sessionState = mock(SessionState.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);

    assertNull(
        IntervalParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.SPANNER));
    assertNotNull(IntervalParser.convertToPG(resultSet, 0, DataFormat.SPANNER));

    assertThrows(
        NullPointerException.class,
        () -> IntervalParser.convertToPG(sessionState, dataOutputStream, resultSet, 0, null));
    assertThrows(NullPointerException.class, () -> IntervalParser.convertToPG(resultSet, 0, null));
  }

  @Test
  public void testToIntervalBinaryInvalid() {
    SpannerException exception =
        assertThrows(SpannerException.class, () -> IntervalParser.toInterval(new byte[15]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());

    assertThrows(NullPointerException.class, () -> IntervalParser.toInterval(new byte[16], null));
  }

  @Test
  public void testStandaloneSignsAndPlacement() {
    assertEquals(Interval.ofSeconds(10), IntervalParser.toInterval("+ 10 seconds"));
    assertEquals(Interval.ofSeconds(-10), IntervalParser.toInterval("- 10 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(14706000000000L)),
        IntervalParser.toInterval("+ 04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-14706000000000L)),
        IntervalParser.toInterval("- 04:05:06"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("+ 1-2"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("+1-2"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("- 1-2"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("-1-2"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(14706000000000L)),
        IntervalParser.toInterval("+04:05:06"));

    assertInvalidInterval("+ + 1 day");
    assertInvalidInterval("1 + day");
    assertInvalidInterval("1 day +");
    assertInvalidInterval("1 2");
  }

  @Test
  public void testTimeAndYearMonthConflicts() {
    assertInvalidInterval("1 day 3 04:05:06");
    assertInvalidInterval("5 1-2");
    assertInvalidInterval("1 month 1-2");
    assertInvalidInterval("1-2 1 month");

    assertInvalidInterval("1 hour 04:05:06");
    assertInvalidInterval("1 minute 04:05:06");
    assertInvalidInterval("1 second 04:05:06");
    assertInvalidInterval("1 ms 04:05:06");
    assertInvalidInterval("1 us 04:05:06");

    assertInvalidInterval("3 ms 5.5 seconds");
    assertInvalidInterval("3 us 5.5 seconds");
    assertInvalidInterval("5.5 seconds 3 ms");
    assertInvalidInterval("5.5 seconds 3 us");

    assertInvalidInterval("04:05:06 1 hour");
    assertInvalidInterval("04:05:06 1 minute");
    assertInvalidInterval("04:05:06 1 second");
    assertInvalidInterval("04:05:06 1 ms");
    assertInvalidInterval("04:05:06 1 us");
  }

  @Test
  public void testTrailingNumberConflicts() {
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 1, BigInteger.valueOf(10_000_000_000L)),
        IntervalParser.toInterval("1 day 10"));
    assertInvalidInterval("10 seconds 5");
    assertInvalidInterval("04:05:06 5");
    assertInvalidInterval("10 ms 5");
    assertInvalidInterval("10 us 5");
  }

  @Test
  public void testFractionalCascading() {
    assertEquals(
        Interval.fromMonthsDaysNanos(1, 7, BigInteger.valueOf(43_200_000_000_000L)),
        IntervalParser.toInterval("1.25 months"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 8, BigInteger.valueOf(64_800_000_000_000L)),
        IntervalParser.toInterval("1.25 weeks"));
  }

  @Test
  public void testFractionalSecondsDigits() {
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_123_456_789L)),
        IntervalParser.toInterval("00:00:01.123456789999"));
  }

  @Test
  public void testIso8601Alternative() {
    assertEquals(
        Interval.fromMonthsDaysNanos(-14, -3, BigInteger.valueOf(-14706000000000L)),
        IntervalParser.toInterval("-P0001-02-03T04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(14, 3, BigInteger.valueOf(14706000000000L)),
        IntervalParser.toInterval("+P0001-02-03T04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(14, 3, BigInteger.valueOf(14706123456000L)),
        IntervalParser.toInterval("P0001-02-03T04:05:06.123456"));
  }

  @Test
  public void testIso8601Variations() {
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("+P1Y"));
    assertEquals(Interval.ofDays(7), IntervalParser.toInterval("P1W"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-14, -3, BigInteger.valueOf(-14706000000000L)),
        IntervalParser.toInterval("P-1Y-2M-3DT-4H-5M-6S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1500000000L)),
        IntervalParser.toInterval("PT1,5S"));
  }

  @Test
  public void testAtAndAgoInvalid() {
    assertInvalidInterval("@");
    assertInvalidInterval("@   ");
    assertInvalidInterval("ago");
    assertInvalidInterval("   ago");
    assertInvalidInterval("@ ago");
    assertInvalidInterval("1 chicago");
  }

  @Test
  public void testUnrecognizedTokens() {
    assertInvalidInterval("1 day % 2 hours");
    assertInvalidInterval("1 day #");
    assertInvalidInterval("1 day trailing");
  }

  @Test
  public void testAllUnitsAndAbbreviations() {
    assertEquals(Interval.ofMonths(12000), IntervalParser.toInterval("1 millennium"));
    assertEquals(Interval.ofMonths(12000), IntervalParser.toInterval("1 millennia"));
    assertEquals(Interval.ofMonths(12000), IntervalParser.toInterval("1 millenniums"));
    assertEquals(Interval.ofMonths(1200), IntervalParser.toInterval("1 century"));
    assertEquals(Interval.ofMonths(1200), IntervalParser.toInterval("1 centuries"));
    assertEquals(Interval.ofMonths(120), IntervalParser.toInterval("1 decade"));
    assertEquals(Interval.ofMonths(120), IntervalParser.toInterval("1 decades"));
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1 year"));
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1 years"));
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1 y"));
    assertEquals(Interval.ofMonths(1), IntervalParser.toInterval("1 month"));
    assertEquals(Interval.ofMonths(1), IntervalParser.toInterval("1 months"));
    assertEquals(Interval.ofMonths(1), IntervalParser.toInterval("1 mon"));
    assertEquals(Interval.ofMonths(1), IntervalParser.toInterval("1 mons"));
    assertEquals(Interval.ofDays(7), IntervalParser.toInterval("1 week"));
    assertEquals(Interval.ofDays(7), IntervalParser.toInterval("1 weeks"));
    assertEquals(Interval.ofDays(7), IntervalParser.toInterval("1 w"));
    assertEquals(Interval.ofDays(1), IntervalParser.toInterval("1 day"));
    assertEquals(Interval.ofDays(1), IntervalParser.toInterval("1 days"));
    assertEquals(Interval.ofDays(1), IntervalParser.toInterval("1 d"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("1 hour"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("1 hours"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("1 h"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(60_000_000_000L)),
        IntervalParser.toInterval("1 minute"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(60_000_000_000L)),
        IntervalParser.toInterval("1 minutes"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(60_000_000_000L)),
        IntervalParser.toInterval("1 min"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(60_000_000_000L)),
        IntervalParser.toInterval("1 mins"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(60_000_000_000L)),
        IntervalParser.toInterval("1 m"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000_000L)),
        IntervalParser.toInterval("1 second"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000_000L)),
        IntervalParser.toInterval("1 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000_000L)),
        IntervalParser.toInterval("1 sec"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000_000L)),
        IntervalParser.toInterval("1 secs"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000_000L)),
        IntervalParser.toInterval("1 s"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000L)),
        IntervalParser.toInterval("1 millisecond"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000L)),
        IntervalParser.toInterval("1 milliseconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000L)),
        IntervalParser.toInterval("1 ms"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000L)),
        IntervalParser.toInterval("1 microsecond"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000L)),
        IntervalParser.toInterval("1 microseconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000L)),
        IntervalParser.toInterval("1 us"));

    assertInvalidInterval("1 millennium 2 millennia");
    assertInvalidInterval("1 century 2 centuries");
    assertInvalidInterval("1 decade 2 decades");
    assertInvalidInterval("1 year 2 y");
    assertInvalidInterval("1 month 2 mons");
    assertInvalidInterval("1 week 2 w");
    assertInvalidInterval("1 day 2 d");
    assertInvalidInterval("1 hour 2 h");
    assertInvalidInterval("1 minute 2 min");
    assertInvalidInterval("1 second 2 sec");
    assertInvalidInterval("1 ms 2 milliseconds");
    assertInvalidInterval("1 us 2 microseconds");
  }

  @Test
  public void testPgjdbcStandardIntervalVectors() {
    assertEquals(
        Interval.fromMonthsDaysNanos(24044, 20, BigInteger.valueOf(-50592100000000L)),
        IntervalParser.toInterval("@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-24044, -20, BigInteger.valueOf(50592100000000L)),
        IntervalParser.toInterval(
            "@ +2004 years -4 mons +20 days -15 hours +57 mins -12.1 secs ago"));
    assertEquals(
        Interval.fromMonthsDaysNanos(24044, 20, BigInteger.valueOf(-57432100000000L)),
        IntervalParser.toInterval("+2004 years -4 mons +20 days -15:57:12.1"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-24044, -20, BigInteger.valueOf(57432100000000L)),
        IntervalParser.toInterval("-2004 years 4 mons -20 days 15:57:12.1"));

    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(100_000L)),
        IntervalParser.toInterval("0.0001 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000L)),
        IntervalParser.toInterval("0.000001 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(700L)),
        IntervalParser.toInterval("0.0000007 seconds"));

    assertEquals(
        Interval.fromMonthsDaysNanos(14, 3, BigInteger.valueOf(14706_000_000_000L)),
        IntervalParser.toInterval("P1Y2M3DT4H5M6S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-10, 3, BigInteger.valueOf(14706_000_000_000L)),
        IntervalParser.toInterval("P-1Y2M3DT4H5M6S"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("P1Y2M"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 3, BigInteger.valueOf(14706_000_000_000L)),
        IntervalParser.toInterval("P3DT4H5M6S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-14, 3, BigInteger.valueOf(-14706_000_000_000L)),
        IntervalParser.toInterval("P-1Y-2M3DT-4H-5M-6S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(6123456000L)),
        IntervalParser.toInterval("PT6.123456S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-6123456000L)),
        IntervalParser.toInterval("PT-6.123456S"));
  }

  @Test
  public void testPostgresRegressionIntervalVectors() {
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("01:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(7200_000_000_000L)),
        IntervalParser.toInterval("+02:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-28800_000_000_000L)),
        IntervalParser.toInterval("-08:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, -1, BigInteger.valueOf(7380_000_000_000L)),
        IntervalParser.toInterval("-1 +02:03"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, -1, BigInteger.valueOf(7380_000_000_000L)),
        IntervalParser.toInterval("-1 days +02:03"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 10, BigInteger.valueOf(43200_000_000_000L)),
        IntervalParser.toInterval("1.5 weeks"));
    assertEquals(
        Interval.fromMonthsDaysNanos(1, 15, BigInteger.ZERO),
        IntervalParser.toInterval("1.5 months"));
    assertEquals(
        Interval.fromMonthsDaysNanos(109, -12, BigInteger.valueOf(47640_000_000_000L)),
        IntervalParser.toInterval("10 years -11 month -12 days +13:14"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 1, BigInteger.valueOf(7384_000_000_000L)),
        IntervalParser.toInterval("1 day 2 hours 3 minutes 4 seconds"));
    assertEquals(Interval.ofMonths(72), IntervalParser.toInterval("6 years"));
    assertEquals(Interval.ofMonths(5), IntervalParser.toInterval("5 months"));
    assertEquals(
        Interval.fromMonthsDaysNanos(5, 0, BigInteger.valueOf(43200_000_000_000L)),
        IntervalParser.toInterval("5 months 12 hours"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 1, BigInteger.valueOf(-3600_000_000_000L)),
        IntervalParser.toInterval("+1 -1:00:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, -1, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("-1 +1:00:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(14, -3, BigInteger.valueOf(14706_789_000_000L)),
        IntervalParser.toInterval("+1-2 -3 +4:05:06.789"));
    assertEquals(
        Interval.fromMonthsDaysNanos(-14, 3, BigInteger.valueOf(-14706_789_000_000L)),
        IntervalParser.toInterval("-1-2 +3 -4:05:06.789"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-80087_660_000_000L)),
        IntervalParser.toInterval("-23 hours 45 min 12.34 sec"));
    assertEquals(
        Interval.fromMonthsDaysNanos(54496, 4, BigInteger.valueOf(1051_000_000_000L)),
        IntervalParser.toInterval(
            "4 millenniums 5 centuries 4 decades 1 year 4 months 4 days 17 minutes 31 seconds"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-100_000_000L)),
        IntervalParser.toInterval("PT-0.1S"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.ZERO), IntervalParser.toInterval("P0Y"));

    assertInvalidInterval("@ 30 eons ago");
    assertInvalidInterval("badly formatted interval");
    assertInvalidInterval("1 second 2 seconds");
    assertInvalidInterval("10 milliseconds 20 milliseconds");
    assertInvalidInterval("5.5 seconds 3 milliseconds");
    assertInvalidInterval("3 milliseconds 5.5 seconds");
    assertInvalidInterval("5.5 seconds 3 microseconds");
    assertInvalidInterval("1:20:05 5 microseconds");
    assertInvalidInterval("1 day 1 day");
    assertInvalidInterval("123 11");
    assertInvalidInterval("123 2:03 -2:04");
    assertInvalidInterval("42 days 2 seconds ago ago");
    assertInvalidInterval("2 minutes ago 5 days");
    assertInvalidInterval("hour 5 months");
    assertInvalidInterval("1 year months days 5 hours");
    assertInvalidInterval("now");
    assertInvalidInterval("today");
    assertInvalidInterval("tomorrow");
    assertInvalidInterval("allballs");
    assertInvalidInterval("epoch");
    assertInvalidInterval("yesterday");
  }

  @Test
  public void testAdditionalIntervalEdgeCases() {
    // Additional unit aliases
    assertEquals(Interval.ofMonths(12000), IntervalParser.toInterval("1 mil"));
    assertEquals(Interval.ofMonths(24000), IntervalParser.toInterval("2 mils"));
    assertEquals(Interval.ofMonths(1200), IntervalParser.toInterval("1 c"));
    assertEquals(Interval.ofMonths(1200), IntervalParser.toInterval("1 cent"));
    assertEquals(Interval.ofMonths(120), IntervalParser.toInterval("1 dec"));
    assertEquals(Interval.ofMonths(240), IntervalParser.toInterval("2 decs"));
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1 yr"));
    assertEquals(Interval.ofMonths(24), IntervalParser.toInterval("2 yrs"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3600_000_000_000L)),
        IntervalParser.toInterval("1 hr"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(7200_000_000_000L)),
        IntervalParser.toInterval("2 hrs"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000_000L)),
        IntervalParser.toInterval("1 msec"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(2_000_000L)),
        IntervalParser.toInterval("2 msecs"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(1_000L)),
        IntervalParser.toInterval("1 usec"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(2_000L)),
        IntervalParser.toInterval("2 usecs"));

    // Fractional ISO 8601 units
    assertEquals(Interval.ofMonths(18), IntervalParser.toInterval("P1.5Y"));
    assertEquals(
        Interval.fromMonthsDaysNanos(1, 15, BigInteger.ZERO), IntervalParser.toInterval("P1.5M"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 10, BigInteger.valueOf(43200_000_000_000L)),
        IntervalParser.toInterval("P1.5W"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 1, BigInteger.valueOf(43200_000_000_000L)),
        IntervalParser.toInterval("P1.5D"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(5400_000_000_000L)),
        IntervalParser.toInterval("PT1.5H"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(90_000_000_000L)),
        IntervalParser.toInterval("PT1.5M"));

    // High-precision ISO seconds (> 9 fractional digits)
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(123456789L)),
        IntervalParser.toInterval("PT0.1234567891S"));

    // Leading dot without preceding sign
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(500_000_000L)),
        IntervalParser.toInterval(".5 seconds"));

    // PT alone is valid (0 interval)
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.ZERO), IntervalParser.toInterval("PT"));

    // Fractional days before time (SQL standard day-time syntax)
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0,
            3,
            BigInteger.valueOf(16 * 3600L + 5 * 60L + 6)
                .multiply(BigInteger.valueOf(1_000_000_000L))),
        IntervalParser.toInterval("3.5 04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0,
            -3,
            BigInteger.valueOf(-(7 * 3600L + 54 * 60L + 54))
                .multiply(BigInteger.valueOf(1_000_000_000L))),
        IntervalParser.toInterval("-3.5 04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0,
            3,
            BigInteger.valueOf(7 * 3600L + 54 * 60L + 54)
                .multiply(BigInteger.valueOf(1_000_000_000L))),
        IntervalParser.toInterval("3.5 -04:05:06"));
    assertEquals(
        Interval.fromMonthsDaysNanos(
            0,
            -3,
            BigInteger.valueOf(-(16 * 3600L + 5 * 60L + 6))
                .multiply(BigInteger.valueOf(1_000_000_000L))),
        IntervalParser.toInterval("-3.5 -04:05:06"));

    // Delimiter-only strings
    assertInvalidInterval(",");
    assertInvalidInterval(", ,");
    assertInvalidInterval(", ago");

    // Double / consecutive signs
    assertInvalidInterval("- -10 seconds");
    assertInvalidInterval("+ +1-2");
    assertInvalidInterval("- -01:00");
    assertInvalidInterval("+ -01:00");
    assertInvalidInterval("- +01:00");

    // Signed leading dots
    assertInvalidInterval("-.5 seconds");
    assertInvalidInterval("+.5 seconds");
    assertInvalidInterval("- .5 seconds");

    // Invalid ISO 8601 strings
    assertInvalidInterval("P");
    assertInvalidInterval("+P");
    assertInvalidInterval("-P");
    assertInvalidInterval("+PT");
    assertInvalidInterval("-PT");

    // Arithmetic overflows
    assertInvalidInterval("18446744073709551617 days");
    assertInvalidInterval("3074457345618258603 years");

    // Mixed sign intervals
    assertEquals(
        Interval.fromMonthsDaysNanos(-1, 5, BigInteger.ZERO),
        IntervalParser.toInterval("-1 month +5 days"));
    assertEquals(
        Interval.fromMonthsDaysNanos(1, -5, BigInteger.ZERO),
        IntervalParser.toInterval("+1 month -5 days"));
  }

  @Test
  public void testYearScaleUnitRounding() {
    // Fractional years round to whole months using HALF_EVEN without cascading into days/seconds
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1.001 years"));
    assertEquals(Interval.ofMonths(13), IntervalParser.toInterval("1.1 years"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.ZERO),
        IntervalParser.toInterval("0.04 years"));
    assertEquals(Interval.ofMonths(1), IntervalParser.toInterval("0.05 years"));
    // Half-way ties round to even integer (rint in PostgreSQL)
    assertEquals(Interval.ofMonths(2), IntervalParser.toInterval("0.125 years")); // 1.5 months -> 2
    assertEquals(Interval.ofMonths(4), IntervalParser.toInterval("0.375 years")); // 4.5 months -> 4
    assertEquals(Interval.ofMonths(-12), IntervalParser.toInterval("-1.001 years"));
    assertEquals(Interval.ofMonths(-13), IntervalParser.toInterval("-1.1 years"));

    // Decades, centuries, and millennia
    assertEquals(Interval.ofMonths(180), IntervalParser.toInterval("1.5 decades"));
    assertEquals(Interval.ofMonths(121), IntervalParser.toInterval("1.01 decades"));
    assertEquals(Interval.ofMonths(1800), IntervalParser.toInterval("1.5 centuries"));
    assertEquals(Interval.ofMonths(1201), IntervalParser.toInterval("1.001 centuries"));
    assertEquals(Interval.ofMonths(18000), IntervalParser.toInterval("1.5 millennia"));
    assertEquals(Interval.ofMonths(12001), IntervalParser.toInterval("1.0001 millennia"));

    // ISO 8601 year rounding
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("P1.001Y"));
    assertEquals(Interval.ofMonths(13), IntervalParser.toInterval("P1.1Y"));
  }

  @Test
  public void testTimeFieldRangeValidation() {
    // Valid time tokens
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(7140_000_000_000L)),
        IntervalParser.toInterval("01:59:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(7140_000_000_000L)),
        IntervalParser.toInterval("01:59"));
    // 60 seconds is valid in PostgreSQL (leap second accommodation, rolls over to 60s)
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3660_000_000_000L)),
        IntervalParser.toInterval("01:00:60"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(3660_500_000_000L)),
        IntervalParser.toInterval("01:00:60.5"));
    // Arbitrary hours are allowed
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(360_000_000_000_000L)),
        IntervalParser.toInterval("100:00:00"));
    assertEquals(
        Interval.fromMonthsDaysNanos(0, 0, BigInteger.valueOf(-360_000_000_000_000L)),
        IntervalParser.toInterval("-100:00:00"));

    // Out of range minutes (>= 60) and seconds (> 60) must be rejected
    assertInvalidInterval("01:60:00");
    assertInvalidInterval("01:60");
    assertInvalidInterval("-01:60:00");
    assertInvalidInterval("+01:60:00");
    assertInvalidInterval("01:00:61");
    assertInvalidInterval("01:00:65");
    assertInvalidInterval("01:99:00");
    assertInvalidInterval("-01:00:61");
  }

  @Test
  public void testSqlStandardYearMonthRangeAndCombinations() {
    // Valid SQL standard Y-M tokens
    assertEquals(Interval.ofMonths(23), IntervalParser.toInterval("1-11"));
    assertEquals(Interval.ofMonths(12), IntervalParser.toInterval("1-0"));
    assertEquals(Interval.ofMonths(-14), IntervalParser.toInterval("-1-2"));
    assertEquals(Interval.ofMonths(14), IntervalParser.toInterval("+1-2"));

    // PostgreSQL permits YEAR units alongside Y-M (only MONTH mask is set for Y-M)
    assertEquals(Interval.ofMonths(26), IntervalParser.toInterval("1 year 1-2"));
    assertEquals(Interval.ofMonths(26), IntervalParser.toInterval("1-2 1 year"));
    assertEquals(Interval.ofMonths(38), IntervalParser.toInterval("1-2 2 years"));

    // Months out of range (>= 12) must be rejected
    assertInvalidInterval("1-12");
    assertInvalidInterval("-1-12");
    assertInvalidInterval("+1-12");
    assertInvalidInterval("1-15");
    assertInvalidInterval("-1-15");

    // Duplicate YEAR is rejected
    assertInvalidInterval("1 year 1-2 1 year");
  }
}
