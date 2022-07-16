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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
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
  public void testGetSpannerValue_InvalidBytesValue() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.bytes(), "value", CopyInParser.createDefaultTimestampUtils()));
  }

  @Test
  public void testGetSpannerValue_InvalidNumberValue() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.int64(), "value", CopyInParser.createDefaultTimestampUtils()));
  }

  @Test
  public void testGetSpannerValue_InvalidBoolValue() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.bool(), "value", CopyInParser.createDefaultTimestampUtils()));
  }

  @Test
  public void testGetSpannerValue_InvalidDateValue() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.date(), "value", CopyInParser.createDefaultTimestampUtils()));
  }

  @Test
  public void testGetSpannerValue_InvalidTimestampValue() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.timestamp(), "value", CopyInParser.createDefaultTimestampUtils()));
  }

  @Test
  public void testGetSpannerValue_UnsupportedType() {
    assertThrows(
        SpannerException.class,
        () ->
            CsvCopyRecord.getSpannerValue(
                Type.struct(StructField.of("f1", Type.string())),
                "value",
                CopyInParser.createDefaultTimestampUtils()));
  }
}
