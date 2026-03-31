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
public class FloatParserTest {

  @Test
  public void testParseFloat() {
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("inf"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("+inf"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("infinity"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("+infinity"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("INF"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("Infinity"), 0.0f);
    assertEquals(Float.POSITIVE_INFINITY, FloatParser.parseFloat("+INF"), 0.0f);

    assertEquals(Float.NEGATIVE_INFINITY, FloatParser.parseFloat("-inf"), 0.0f);
    assertEquals(Float.NEGATIVE_INFINITY, FloatParser.parseFloat("-infinity"), 0.0f);
    assertEquals(Float.NEGATIVE_INFINITY, FloatParser.parseFloat("-INF"), 0.0f);

    assertEquals(Float.NaN, FloatParser.parseFloat("NaN"), 0.0f);
    assertEquals(Float.NaN, FloatParser.parseFloat("nan"), 0.0f);
    assertEquals(Float.NaN, FloatParser.parseFloat("NAN"), 0.0f);

    assertEquals(3.14f, FloatParser.parseFloat("3.14"), 0.0f);
    assertEquals(-3.14f, FloatParser.parseFloat("-3.14"), 0.0f);
    assertEquals(0.0f, FloatParser.parseFloat("0.0"), 0.0f);

    assertThrows(PGException.class, () -> FloatParser.parseFloat(""));
    assertThrows(PGException.class, () -> FloatParser.parseFloat("  "));
    assertThrows(PGException.class, () -> FloatParser.parseFloat("foo"));
    assertThrows(PGException.class, () -> FloatParser.parseFloat(null));
  }

  @Test
  public void testToFloat() {
    float d = new Random().nextFloat();
    byte[] data = new byte[4];
    ByteConverter.float4(data, 0, d);
    assertEquals(d, FloatParser.toFloat(data), 0.0);

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> FloatParser.toFloat(new byte[2]));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testStringParse() {
    assertEquals("3.14", new FloatParser(3.14f).stringParse());
    assertNull(new FloatParser(null).stringParse());
    assertEquals(
        "123.456",
        new FloatParser("123.456".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT).stringParse());
    assertThrows(
        PGException.class,
        () -> new FloatParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));

    assertEquals("NaN", new FloatParser(Float.NaN).stringParse());
    assertEquals("Infinity", new FloatParser(Float.POSITIVE_INFINITY).stringParse());
    assertEquals("-Infinity", new FloatParser(Float.NEGATIVE_INFINITY).stringParse());
  }

  @Test
  public void testToValue() {
    assertEquals(Value.float32(3.14f), FloatParser.toValue(3.14f));

    assertEquals(Value.string("NaN"), FloatParser.toValue(Float.NaN));
    assertEquals(Value.string("Infinity"), FloatParser.toValue(Float.POSITIVE_INFINITY));
    assertEquals(Value.string("-Infinity"), FloatParser.toValue(Float.NEGATIVE_INFINITY));

    assertEquals(Value.string("NaN"), FloatParser.toValue(FloatParser.parseFloat("NaN")));
    assertEquals(Value.string("Infinity"), FloatParser.toValue(FloatParser.parseFloat("Inf")));
  }

  @Test
  public void testBind() {
    ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
    new FloatParser(Float.NaN).bind(builder, "nan");
    new FloatParser(Float.POSITIVE_INFINITY).bind(builder, "inf");
    new FloatParser(Float.NEGATIVE_INFINITY).bind(builder, "-inf");

    ImmutableMap<String, Value> parameters = builder.build();
    assertEquals(Value.string("NaN"), parameters.get("nan"));
    assertEquals(Value.string("Infinity"), parameters.get("inf"));
    assertEquals(Value.string("-Infinity"), parameters.get("-inf"));
  }
}
