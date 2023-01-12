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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class LongParserTest {

  @Test
  public void testToLong() {
    long l = new Random().nextLong();
    byte[] data = new byte[8];
    ByteConverter.int8(data, 0, l);
    assertEquals(l, LongParser.toLong(data));

    // We allow 4 byte values to be parsed as a long as well, as Cloud Spanner allows int4 as a
    // valid data type for a column in a DDL statement, but it is converted to an int8 on the
    // backend.
    int i = new Random().nextInt();
    data = new byte[4];
    ByteConverter.int4(data, 0, i);
    assertEquals(i, LongParser.toLong(data));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> LongParser.toLong(new byte[2]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals("100", new LongParser(100L).stringParse());
    assertNull(new LongParser(null).stringParse());
    assertThrows(
        PGException.class,
        () -> new LongParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testCreate() {
    assertEquals(
        0L, new LongParser("0".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        1L, new LongParser("1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        100L,
        new LongParser("100".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        Long.MAX_VALUE,
        new LongParser(
                String.valueOf(Long.MAX_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        Long.MIN_VALUE,
        new LongParser(
                String.valueOf(Long.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        0L,
        new LongParser("0.1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        2L,
        new LongParser("1.5".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        101L,
        new LongParser("100.999".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());

    assertThrows(
        PGException.class,
        () ->
            new LongParser(
                (Long.MAX_VALUE + "0").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
    assertThrows(
        PGException.class,
        () ->
            new LongParser(
                (Long.MIN_VALUE + "0").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }
}
