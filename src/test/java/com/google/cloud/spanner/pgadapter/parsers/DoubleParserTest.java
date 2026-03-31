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
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class DoubleParserTest {

  @Test
  public void testParseDouble() {
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("inf"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("+inf"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("infinity"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("+infinity"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("INF"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("Infinity"), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, DoubleParser.parseDouble("+INF"), 0.0);

    assertEquals(Double.NEGATIVE_INFINITY, DoubleParser.parseDouble("-inf"), 0.0);
    assertEquals(Double.NEGATIVE_INFINITY, DoubleParser.parseDouble("-infinity"), 0.0);
    assertEquals(Double.NEGATIVE_INFINITY, DoubleParser.parseDouble("-INF"), 0.0);

    assertEquals(Double.NaN, DoubleParser.parseDouble("NaN"), 0.0);
    assertEquals(Double.NaN, DoubleParser.parseDouble("nan"), 0.0);
    assertEquals(Double.NaN, DoubleParser.parseDouble("NAN"), 0.0);

    assertEquals(3.14, DoubleParser.parseDouble("3.14"), 0.0);
    assertEquals(-3.14, DoubleParser.parseDouble("-3.14"), 0.0);
    assertEquals(0.0, DoubleParser.parseDouble("0.0"), 0.0);

    assertThrows(PGException.class, () -> DoubleParser.parseDouble(""));
    assertThrows(PGException.class, () -> DoubleParser.parseDouble("  "));
    assertThrows(PGException.class, () -> DoubleParser.parseDouble("foo"));
    assertThrows(PGException.class, () -> DoubleParser.parseDouble(null));
  }

  @Test
  public void testToValue() {
    assertEquals(Value.float64(3.14), DoubleParser.toValue(3.14));

    assertEquals(Value.string("NaN"), DoubleParser.toValue(Double.NaN));
    assertEquals(Value.string("Infinity"), DoubleParser.toValue(Double.POSITIVE_INFINITY));
    assertEquals(Value.string("-Infinity"), DoubleParser.toValue(Double.NEGATIVE_INFINITY));

    assertEquals(Value.string("NaN"), DoubleParser.toValue(DoubleParser.parseDouble("NaN")));
    assertEquals(Value.string("Infinity"), DoubleParser.toValue(DoubleParser.parseDouble("Inf")));
  }

  @Test
  public void testToDouble() {
    double d = new Random().nextDouble();
    byte[] data = new byte[8];
    ByteConverter.float8(data, 0, d);
    assertEquals(d, DoubleParser.toDouble(data), 0.0);

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> DoubleParser.toDouble(new byte[4]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals("3.14", new DoubleParser(3.14).stringParse());
    assertNull(new DoubleParser(null).stringParse());
    assertThrows(
        PGException.class,
        () -> new DoubleParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));

    assertEquals("NaN", new DoubleParser(Double.NaN).stringParse());
    assertEquals("Infinity", new DoubleParser(Double.POSITIVE_INFINITY).stringParse());
    assertEquals("-Infinity", new DoubleParser(Double.NEGATIVE_INFINITY).stringParse());
  }

  @Test
  public void testBind() {
    ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
    new DoubleParser(Double.NaN).bind(builder, "nan");
    new DoubleParser(Double.POSITIVE_INFINITY).bind(builder, "inf");
    new DoubleParser(Double.NEGATIVE_INFINITY).bind(builder, "-inf");

    ImmutableMap<String, Value> parameters = builder.build();
    assertEquals(Value.string("NaN"), parameters.get("nan"));
    assertEquals(Value.string("Infinity"), parameters.get("inf"));
    assertEquals(Value.string("-Infinity"), parameters.get("-inf"));
  }
}
