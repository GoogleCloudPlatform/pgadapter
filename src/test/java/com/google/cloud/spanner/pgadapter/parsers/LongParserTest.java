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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
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

  @Test(timeout = 2000)
  public void testScientificNotationAndLargeInputs() {
    assertEquals(
        100L,
        new LongParser("1e2".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        150L,
        new LongParser("1.5e2".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        -25L,
        new LongParser("-2.5E1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        1L,
        new LongParser("0.5".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        -1L,
        new LongParser("-0.5".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        0L,
        new LongParser("0e100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        0L,
        new LongParser("0e-100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        0L,
        new LongParser("1e-100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        0L,
        new LongParser("-1e-1000000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        42L,
        new LongParser(" 42 ".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        -2L,
        new LongParser("\t-1.5\n".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        0L,
        new LongParser("0.09".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        0L,
        new LongParser("-0.09".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.longValue());
    assertEquals(
        0L,
        new LongParser("1e-2147483647".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        1L,
        new LongParser(
                ("1." + new String(new char[125]).replace('\0', '0') + "5")
                    .getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        42L,
        new LongParser(
                (new String(new char[200]).replace('\0', '0') + "42")
                    .getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        Long.MAX_VALUE,
        new LongParser((Long.MAX_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());
    assertEquals(
        Long.MIN_VALUE,
        new LongParser((Long.MIN_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.longValue());

    for (String overflowValue :
        new String[] {
          "1e10000000",
          "1e100000000",
          "1e1000000000",
          "-1e100000000",
          "1e2147483647",
          "10e2147483647",
          "1e19",
          "1e20",
          Long.MAX_VALUE + ".5",
          Long.MIN_VALUE + ".5",
          "-9223372036854775809",
          "+9223372036854775808",
          " 99999999999999999999 ",
          "+99999999999999999999",
          "1" + new String(new char[1024]).replace('\0', '0'),
        }) {
      PGException exception =
          assertThrows(
              PGException.class,
              () ->
                  new LongParser(overflowValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
      assertEquals(SQLState.NumericValueOutOfRange, exception.getSQLState());
      if (overflowValue.trim().length() > 128) {
        assertTrue(exception.getMessage().endsWith("..."));
        assertTrue(exception.getMessage().length() <= "Invalid int8 value: ".length() + 131);
      } else {
        assertFalse(exception.getMessage().endsWith("..."));
      }
    }

    for (String syntaxErrorValue :
        new String[] {
          "",
          "   ",
          "+",
          "-",
          "1e999999999999",
          "1e-2147483648",
          "1." + new String(new char[126]).replace('\0', '0') + "5",
          "1." + new String(new char[500_000]).replace('\0', '0') + "5",
          "1" + new String(new char[500_000]).replace('\0', '0') + "e-500000",
          new String(new char[127]).replace('\0', 'a') + "\uD83D\uDE00",
        }) {
      PGException exception =
          assertThrows(
              PGException.class,
              () ->
                  new LongParser(
                      syntaxErrorValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
      assertEquals(SQLState.SyntaxError, exception.getSQLState());
      if (syntaxErrorValue.trim().length() > 128) {
        assertTrue(exception.getMessage().endsWith("..."));
        assertTrue(exception.getMessage().length() <= "Invalid int8 value: ".length() + 131);
        assertFalse(
            Character.isHighSurrogate(
                exception.getMessage().charAt(exception.getMessage().length() - 4)));
      } else {
        assertFalse(exception.getMessage().endsWith("..."));
      }
    }
  }
}
