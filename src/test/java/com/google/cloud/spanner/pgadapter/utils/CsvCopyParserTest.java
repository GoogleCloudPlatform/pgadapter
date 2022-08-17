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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.utils.CsvCopyParser.CsvCopyRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CsvCopyParserTest {

  @Test
  public void testCanCreateIterator() throws IOException {
    CsvCopyParser parser =
        new CsvCopyParser(CSVFormat.POSTGRESQL_TEXT, new PipedOutputStream(), 256, false);
    assertNotNull(parser.iterator());
    parser.close();
  }

  @Test
  public void testCanCreateIteratorWithHeader() throws IOException {
    PipedOutputStream outputStream = new PipedOutputStream();
    DataOutputStream data = new DataOutputStream(outputStream);
    new Thread(
            () -> {
              while (true) {
                try {
                  data.write("\"col1\"\t\"col2\"\n".getBytes(StandardCharsets.UTF_8));
                  break;
                } catch (IOException e) {
                  if (e.getMessage().contains("Pipe not connected")) {
                    Thread.yield();
                  } else {
                    throw new RuntimeException(e);
                  }
                }
              }
            })
        .start();
    CsvCopyParser parser = new CsvCopyParser(CSVFormat.POSTGRESQL_TEXT, outputStream, 256, true);
    assertNotNull(parser.iterator());
    parser.close();
  }

  @Test
  public void testGetSpannerValueBool() {
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "t"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "tr"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "tru"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "true"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "1"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "on"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "y"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "ye"));
    assertEquals(Value.bool(true), CsvCopyRecord.getSpannerValue(Type.bool(), "yes"));

    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "f"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "fa"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "fal"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "fals"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "false"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "0"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "off"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "of"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "n"));
    assertEquals(Value.bool(false), CsvCopyRecord.getSpannerValue(Type.bool(), "no"));

    assertEquals(Value.bool(null), CsvCopyRecord.getSpannerValue(Type.bool(), null));
  }

  @Test
  public void testGetSpannerValueBytes() {
    assertEquals(
        Value.bytes(ByteArray.copyFrom("test")),
        CsvCopyRecord.getSpannerValue(Type.bytes(), "\\x74657374"));
    assertEquals(
        Value.bytes(ByteArray.copyFrom("")), CsvCopyRecord.getSpannerValue(Type.bytes(), "\\x"));
    assertEquals(Value.bytes(null), CsvCopyRecord.getSpannerValue(Type.bytes(), null));
  }

  @Test
  public void testGetSpannerValueInt64() {
    assertEquals(Value.int64(-1L), CsvCopyRecord.getSpannerValue(Type.int64(), "-1"));
    assertEquals(Value.int64(1L), CsvCopyRecord.getSpannerValue(Type.int64(), "1"));
    assertEquals(Value.int64(0L), CsvCopyRecord.getSpannerValue(Type.int64(), "0"));
    assertEquals(
        Value.int64(Long.MAX_VALUE),
        CsvCopyRecord.getSpannerValue(Type.int64(), "9223372036854775807"));
    assertEquals(
        Value.int64(Long.MIN_VALUE),
        CsvCopyRecord.getSpannerValue(Type.int64(), "-9223372036854775808"));
    assertEquals(Value.int64(null), CsvCopyRecord.getSpannerValue(Type.int64(), null));
  }

  @Test
  public void testGetSpannerValueFloat64() {
    assertEquals(Value.float64(-1.0D), CsvCopyRecord.getSpannerValue(Type.float64(), "-1.0"));
    assertEquals(Value.float64(0.0D), CsvCopyRecord.getSpannerValue(Type.float64(), "0.0"));
    assertEquals(Value.float64(1.0D), CsvCopyRecord.getSpannerValue(Type.float64(), "1.0"));
    assertEquals(Value.float64(null), CsvCopyRecord.getSpannerValue(Type.float64(), null));
  }

  @Test
  public void testGetSpannerValueNumeric() {
    assertEquals(Value.pgNumeric("-1.0"), CsvCopyRecord.getSpannerValue(Type.pgNumeric(), "-1.0"));
    assertEquals(Value.pgNumeric("0.0"), CsvCopyRecord.getSpannerValue(Type.pgNumeric(), "0.0"));
    assertEquals(Value.pgNumeric("1.0"), CsvCopyRecord.getSpannerValue(Type.pgNumeric(), "1.0"));
    assertEquals(Value.pgNumeric(null), CsvCopyRecord.getSpannerValue(Type.pgNumeric(), null));
  }

  @Test
  public void testGetSpannerValueString() {
    assertEquals(Value.string("test"), CsvCopyRecord.getSpannerValue(Type.string(), "test"));
    assertEquals(Value.string(""), CsvCopyRecord.getSpannerValue(Type.string(), ""));
    assertEquals(Value.string(null), CsvCopyRecord.getSpannerValue(Type.string(), null));
  }

  @Test
  public void testGetSpannerValueDate() {
    assertEquals(
        Value.date(Date.parseDate("2022-08-17")),
        CsvCopyRecord.getSpannerValue(Type.date(), "2022-08-17"));
    assertEquals(Value.date(null), CsvCopyRecord.getSpannerValue(Type.date(), null));
  }

  @Test
  public void testGetSpannerValueTimestamp() {
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2093-08-02T14:53:40.481913Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2093-08-02T14:53:40.481913+00"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17T10:11:12.123456789Z"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17 10:11:12.123456789Z"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17 10:11:12.123456789+00"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17 10:11:12.123456789+00:00"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17T10:11:12.123456789+00"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17T10:11:12.123456789+00:00"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17 12:11:12.123456789+02"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17T12:11:12.123456789+02"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17 08:11:12.123456789-02"));
    assertEquals(
        Value.timestamp(Timestamp.parseTimestamp("2022-08-17T10:11:12.123456789Z")),
        CsvCopyRecord.getSpannerValue(Type.timestamp(), "2022-08-17T08:11:12.123456789-02"));
    assertEquals(Value.date(null), CsvCopyRecord.getSpannerValue(Type.date(), null));
  }

  @Test
  public void testGetSpannerValue_InvalidBytesValue() {
    assertThrows(
        SpannerException.class, () -> CsvCopyRecord.getSpannerValue(Type.bytes(), "value"));
  }

  @Test
  public void testGetSpannerValue_InvalidNumberValue() {
    assertThrows(
        SpannerException.class, () -> CsvCopyRecord.getSpannerValue(Type.int64(), "value"));
  }

  @Test
  public void testGetSpannerValue_InvalidBoolValue() {
    assertThrows(SpannerException.class, () -> CsvCopyRecord.getSpannerValue(Type.bool(), "value"));
  }

  @Test
  public void testGetSpannerValue_InvalidDateValue() {
    assertThrows(SpannerException.class, () -> CsvCopyRecord.getSpannerValue(Type.date(), "value"));
  }

  @Test
  public void testGetSpannerValue_InvalidTimestampValue() {
    assertThrows(
        SpannerException.class, () -> CsvCopyRecord.getSpannerValue(Type.timestamp(), "value"));
  }

  @Test
  public void testGetSpannerValue_UnsupportedType() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.struct(StructField.of("f1", Type.string())), "value"));
  }
}
