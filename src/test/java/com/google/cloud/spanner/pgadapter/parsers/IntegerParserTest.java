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
public class IntegerParserTest {

  @Test
  public void testStringParse() {
    assertEquals("100", new IntegerParser(100).stringParse());
    assertNull(new IntegerParser(null).stringParse());
    assertThrows(
        PGException.class,
        () -> new IntegerParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testCreate() {
    assertEquals(
        0,
        new IntegerParser("0".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        1,
        new IntegerParser("1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        100,
        new IntegerParser("100".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        Integer.MAX_VALUE,
        new IntegerParser(
                String.valueOf(Integer.MAX_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        Integer.MIN_VALUE,
        new IntegerParser(
                String.valueOf(Integer.MIN_VALUE).getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        0,
        new IntegerParser("0.1".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        2,
        new IntegerParser("1.5".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        101,
        new IntegerParser("100.999".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());

    assertThrows(
        PGException.class,
        () ->
            new IntegerParser(
                String.valueOf(1L + (long) Integer.MAX_VALUE).getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT));
    assertThrows(
        PGException.class,
        () ->
            new IntegerParser(
                String.valueOf((long) Integer.MIN_VALUE - 1L).getBytes(StandardCharsets.UTF_8),
                FormatCode.TEXT));
  }

  @Test(timeout = 2000)
  public void testScientificNotationAndLargeInputs() {
    assertEquals(
        100,
        new IntegerParser("1e2".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).item.intValue());
    assertEquals(
        0,
        new IntegerParser("1e-100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        0,
        new IntegerParser("0e100000000".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        42,
        new IntegerParser(" 42 ".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        Integer.MAX_VALUE,
        new IntegerParser(
                (Integer.MAX_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());
    assertEquals(
        Integer.MIN_VALUE,
        new IntegerParser(
                (Integer.MIN_VALUE + ".4").getBytes(StandardCharsets.UTF_8), FormatCode.TEXT)
            .item.intValue());

    for (String overflowValue :
        new String[] {
          "1e100000000",
          "-1e100000000",
          "1e2147483647",
          "1e10",
          Integer.MAX_VALUE + ".5",
          Integer.MIN_VALUE + ".5",
          " 2147483648 ",
          "1" + new String(new char[1024]).replace('\0', '0'),
        }) {
      PGException exception =
          assertThrows(
              PGException.class,
              () ->
                  new IntegerParser(
                      overflowValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
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
                  new IntegerParser(
                      syntaxErrorValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
      assertEquals(SQLState.SyntaxError, exception.getSQLState());
    }
  }
}
