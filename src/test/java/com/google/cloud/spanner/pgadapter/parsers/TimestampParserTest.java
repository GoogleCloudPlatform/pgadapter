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

import static com.google.cloud.spanner.pgadapter.parsers.Parser.PG_EPOCH_SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class TimestampParserTest {

  @Test
  public void testToTimestamp() {
    long micros = new Random().nextLong();
    if (micros < -62135596800000L) {
      micros = -62135596800000L;
    } else if (micros > 253402300799000L) {
      micros = 253402300799000L;
    }
    byte[] data = new byte[8];
    ByteConverter.int8(data, 0, micros - PG_EPOCH_SECONDS * 1000_000L);
    assertEquals(Timestamp.ofTimeMicroseconds(micros), TimestampParser.toTimestamp(data));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> TimestampParser.toTimestamp(new byte[4]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());

    assertArrayEquals(
        data,
        new TimestampParser(TimestampParser.toTimestamp(data), mock(SessionState.class))
            .binaryParse());
    assertNull(new TimestampParser(null, mock(SessionState.class)).binaryParse());
  }

  @Test
  public void testSpannerParse() {
    assertEquals(
        "2022-07-08T07:22:59.123456789Z",
        new TimestampParser(
                "2022-07-08 07:22:59.123456789+00".getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT,
                mock(SessionState.class))
            .spannerParse());
    assertNull(new TimestampParser(null, mock(SessionState.class)).spannerParse());

    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("ts", Type.timestamp())),
            ImmutableList.of(
                Struct.newBuilder()
                    .set("ts")
                    .to(Timestamp.parseTimestamp("2022-07-08T07:22:59.123456789Z"))
                    .build()));
    resultSet.next();
    assertArrayEquals(
        "2022-07-08T07:22:59.123456789Z".getBytes(StandardCharsets.UTF_8),
        TimestampParser.convertToPG(resultSet, 0, DataFormat.SPANNER, ZoneId.of("UTC")));
  }

  @Test
  public void testConvertToPGStream() throws IOException {
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(StructField.of("ts", Type.timestamp())),
            ImmutableList.of(
                Struct.newBuilder()
                    .set("ts")
                    .to(Timestamp.parseTimestamp("2022-07-08T07:22:59.123456789Z"))
                    .build()));
    resultSet.next();

    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);

    // Text format
    assertNull(
        TimestampParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] textBytes =
        TimestampParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT, ZoneId.of("UTC"));
    ByteArrayOutputStream expectedText = new ByteArrayOutputStream();
    DataOutputStream expectedTextStream = new DataOutputStream(expectedText);
    expectedTextStream.writeInt(textBytes.length);
    expectedTextStream.write(textBytes);
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        TimestampParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    byte[] binaryBytes =
        TimestampParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY, ZoneId.of("UTC"));
    ByteArrayOutputStream expectedBinary = new ByteArrayOutputStream();
    DataOutputStream expectedBinaryStream = new DataOutputStream(expectedBinary);
    expectedBinaryStream.writeInt(binaryBytes.length);
    expectedBinaryStream.write(binaryBytes);
    assertArrayEquals(expectedBinary.toByteArray(), output.toByteArray());
    output.reset();

    // Spanner format
    assertNull(
        TimestampParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.SPANNER));
    byte[] spannerBytes =
        TimestampParser.convertToPG(resultSet, 0, DataFormat.SPANNER, ZoneId.of("UTC"));
    ByteArrayOutputStream expectedSpanner = new ByteArrayOutputStream();
    DataOutputStream expectedSpannerStream = new DataOutputStream(expectedSpanner);
    expectedSpannerStream.writeInt(spannerBytes.length);
    expectedSpannerStream.write(spannerBytes);
    assertArrayEquals(expectedSpanner.toByteArray(), output.toByteArray());
    output.reset();
  }

  @Test
  public void testConvertToPGPre2000EpochAndNonUtcTimezone() throws IOException {
    Timestamp pre2000 = Timestamp.parseTimestamp("1970-01-01T00:00:00Z");
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getTimestamp(0)).thenReturn(pre2000);

    SessionState sessionState = mock(SessionState.class);
    ZoneId nyZone = ZoneId.of("America/New_York");
    when(sessionState.getTimezone()).thenReturn(nyZone);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);

    // Binary format (pre-2000 produces negative microsecond offset relative to PG epoch 2000-01-01)
    assertNull(
        TimestampParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    byte[] binaryBytes =
        TimestampParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY, nyZone);
    ByteArrayOutputStream expectedBinary = new ByteArrayOutputStream();
    DataOutputStream expectedBinaryStream = new DataOutputStream(expectedBinary);
    expectedBinaryStream.writeInt(8);
    expectedBinaryStream.write(binaryBytes);
    assertArrayEquals(expectedBinary.toByteArray(), output.toByteArray());
    output.reset();

    // Text format with non-UTC timezone
    assertNull(
        TimestampParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] textBytes =
        TimestampParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT, nyZone);
    ByteArrayOutputStream expectedText = new ByteArrayOutputStream();
    DataOutputStream expectedTextStream = new DataOutputStream(expectedText);
    expectedTextStream.writeInt(textBytes.length);
    expectedTextStream.write(textBytes);
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
  }

  @Test
  public void testStringParse() {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("+00"));
    assertEquals(
        "2022-07-08 07:22:59.123456+00",
        new TimestampParser(
                Timestamp.parseTimestamp("2022-07-08T07:22:59.123456789Z"), sessionState)
            .stringParse());
    assertNull(new TimestampParser(null, sessionState).stringParse());
    assertThrows(
        PGException.class,
        () ->
            new TimestampParser(
                "foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT, sessionState));
  }

  @Test
  public void testTextToTimestamp() {
    assertEquals(
        Timestamp.parseTimestamp("2022-10-09T19:09:18Z"),
        TimestampParser.toTimestamp("2022-10-09 10:09:18", ZoneId.of("-09:00")));
    assertEquals(
        Timestamp.parseTimestamp("2022-12-28T09:00Z"),
        TimestampParser.toTimestamp("2022-12-28 10:00", ZoneId.of("CET")));
    assertEquals(
        Timestamp.parseTimestamp("2022-08-28T08:00Z"),
        TimestampParser.toTimestamp("2022-08-28 10:00", ZoneId.of("CET")));
    assertEquals(
        Timestamp.parseTimestamp("2022-08-28T08:00Z"),
        TimestampParser.toTimestamp("2022-08-28 10:00", ZoneId.of("Europe/Amsterdam")));
    assertEquals(
        Timestamp.parseTimestamp("2022-12-27T14:00:00Z"),
        TimestampParser.toTimestamp("2022-12-28", ZoneId.of("+10:00")));
    assertEquals(
        Timestamp.parseTimestamp("2022-12-28T08:00Z"),
        TimestampParser.toTimestamp("2022-12-28 10:00+02:00", ZoneId.of("CET")));
    assertEquals(
        Timestamp.parseTimestamp("2022-12-28T07:30Z"),
        TimestampParser.toTimestamp("2022-12-28 10:00+02:30", ZoneId.of("CET")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("(\"2011-11-04 00:05:23.123456+00:00\")", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("('2011-11-04 00:05:23.123456+00:00')", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("'2011-11-04 00:05:23.123456+00:00'", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("'2011-11-04 00:05:23.123456 +00:00'", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp("", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp("(", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp(")", ZoneId.of("UTC")));
    assertThrows(
        PGException.class,
        () -> TimestampParser.toTimestamp("'2011-11-04 00:05:23.123456+00:00')", ZoneId.of("UTC")));
    assertThrows(
        PGException.class,
        () -> TimestampParser.toTimestamp("('2011-11-04 00:05:23.123456+00:00'", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp("()", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp("''", ZoneId.of("UTC")));
    assertThrows(PGException.class, () -> TimestampParser.toTimestamp("'2000'", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2000-01-01T00:00:00Z"),
        TimestampParser.toTimestamp("'2000-01-01'", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp(" (\"2011-11-04 00:05:23.123456+00:00\")", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("(\"2011-11-04 00:05:23.123456+00:00\") ", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("( \"2011-11-04 00:05:23.123456+00:00\" )", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp("(\" 2011-11-04 00:05:23.123456+00:00\")", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp(
            "\n(  \"2011-11-04 00:05:23.123456+00:00  \" )", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp(
            "\t\n( \"  2011-11-04 00:05:23.123456+00:00  \n\t\" )", ZoneId.of("UTC")));
    assertEquals(
        Timestamp.parseTimestamp("2011-11-04T00:05:23.123456Z"),
        TimestampParser.toTimestamp(
            "\t\n( \"  2011-11-04 00:05:23.123456 +00:00  \n\t\" )", ZoneId.of("UTC")));
  }
}
