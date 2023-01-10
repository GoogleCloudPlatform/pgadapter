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

import com.google.cloud.Date;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DateParserTest {

  @Test
  public void testToDate() {
    assertEquals(Date.fromYearMonthDay(2000, 1, 1), DateParser.toDate(new byte[] {0, 0, 0, 0}));
    assertEquals(Date.fromYearMonthDay(2000, 1, 2), DateParser.toDate(new byte[] {0, 0, 0, 1}));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> DateParser.toDate(new byte[] {}));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals("2022-07-08", new DateParser(Date.fromYearMonthDay(2022, 7, 8)).stringParse());
    assertNull(new DateParser(null).stringParse());
    assertThrows(
        PGException.class,
        () -> new DateParser("2022".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testToString() {
    assertEquals("2023-01-06", DateParser.toString(Date.fromYearMonthDay(2023, 1, 6)));
    assertEquals("0900-01-06", DateParser.toString(Date.fromYearMonthDay(900, 1, 6)));
    assertEquals("0016-02-28", DateParser.toString(Date.fromYearMonthDay(16, 2, 28)));
    assertEquals("0001-01-01", DateParser.toString(Date.fromYearMonthDay(1, 1, 1)));
    assertEquals("+10000-01-01", DateParser.toString(Date.fromYearMonthDay(10000, 1, 1)));
  }
}
