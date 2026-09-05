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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Interval;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
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
  }
}
