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

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShortParserTest {

  @Test
  public void testStringParse() {
    assertEquals("100", new ShortParser((short) 100).stringParse());
    assertNull(new ShortParser(null).stringParse());
    assertEquals(
        "-200",
        new ShortParser("-200".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).stringParse());
    assertThrows(
        PGException.class,
        () -> new ShortParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testCreate() {
    assertEquals(
        0,
        new ShortParser("0".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        1,
        new ShortParser("1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        100,
        new ShortParser("100".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        Short.MAX_VALUE,
        new ShortParser(
                String.valueOf(Short.MAX_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        Short.MIN_VALUE,
        new ShortParser(
                String.valueOf(Short.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        0,
        new ShortParser("0.1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        2,
        new ShortParser("1.5".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        101,
        new ShortParser("100.999".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());

    assertThrows(
        PGException.class,
        () ->
            new ShortParser(
                String.valueOf(1 + (int) Short.MAX_VALUE).getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT));
    assertThrows(
        PGException.class,
        () ->
            new ShortParser(
                String.valueOf((int) Short.MIN_VALUE - 1L).getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT));
  }

  @Test(timeout = 2000)
  public void testScientificNotationAndLargeInputs() {
    assertEquals(
        100,
        new ShortParser("1e2".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.shortValue());
    assertEquals(
        0,
        new ShortParser("1e-100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        0,
        new ShortParser("0e100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        42,
        new ShortParser(" 42 ".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        Short.MAX_VALUE,
        new ShortParser((Short.MAX_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());
    assertEquals(
        Short.MIN_VALUE,
        new ShortParser((Short.MIN_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.shortValue());

    for (String overflowValue :
        new String[] {
          "1e100000000",
          "-1e100000000",
          "1e2147483647",
          "1e5",
          Short.MAX_VALUE + ".5",
          Short.MIN_VALUE + ".5",
          " 32768 ",
          "1" + new String(new char[1024]).replace('\0', '0'),
        }) {
      PGException exception =
          assertThrows(
              PGException.class,
              () ->
                  new ShortParser(overflowValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
      assertEquals(SQLState.NumericValueOutOfRange, exception.getSQLState());
    }

    for (String syntaxErrorValue :
        new String[] {
          "   ",
          "1e-2147483648",
          "1." + new String(new char[128]).replace('\0', '0') + "5",
          "1." + new String(new char[100_000]).replace('\0', '0') + "5",
        }) {
      PGException exception =
          assertThrows(
              PGException.class,
              () ->
                  new ShortParser(
                      syntaxErrorValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
      assertEquals(SQLState.SyntaxError, exception.getSQLState());
    }
  }
}
