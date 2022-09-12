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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BooleanParserTest {

  @Test
  public void testBinaryToBoolean() {
    assertTrue(BooleanParser.toBoolean(new byte[] {1}));
    assertTrue(BooleanParser.toBoolean(new byte[] {1, 0}));

    assertFalse(BooleanParser.toBoolean(new byte[] {0}));
    assertFalse(BooleanParser.toBoolean(new byte[] {0, 1}));
    assertFalse(BooleanParser.toBoolean(new byte[] {0, 0}));
    assertFalse(BooleanParser.toBoolean(new byte[] {2}));
    assertFalse(BooleanParser.toBoolean(new byte[] {-1}));

    SpannerException spannerException =
        assertThrows(SpannerException.class, () -> BooleanParser.toBoolean(new byte[] {}));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testToBoolean() {
    assertTrue(BooleanParser.toBoolean("true"));
    assertTrue(BooleanParser.toBoolean("tru"));
    assertTrue(BooleanParser.toBoolean("tr"));
    assertTrue(BooleanParser.toBoolean("t"));

    assertFalse(BooleanParser.toBoolean("false"));
    assertFalse(BooleanParser.toBoolean("fals"));
    assertFalse(BooleanParser.toBoolean("fal"));
    assertFalse(BooleanParser.toBoolean("fa"));
    assertFalse(BooleanParser.toBoolean("f"));

    assertThrows(PGException.class, () -> BooleanParser.toBoolean("foo"));
    assertThrows(
        PGException.class,
        () -> new BooleanParser("bar".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testBinaryParse() {
    assertArrayEquals(new byte[] {1}, new BooleanParser(Boolean.TRUE).binaryParse());
    assertArrayEquals(new byte[] {0}, new BooleanParser(Boolean.FALSE).binaryParse());
    assertNull(new BooleanParser(null).binaryParse());
  }

  @Test
  public void testStringParse() {
    assertEquals("t", new BooleanParser(Boolean.TRUE).stringParse());
    assertEquals("f", new BooleanParser(Boolean.FALSE).stringParse());
    assertNull(new BooleanParser(null).stringParse());
  }

  @Test
  public void testSpannerParse() {
    assertEquals("true", new BooleanParser(Boolean.TRUE).spannerParse());
    assertEquals("false", new BooleanParser(Boolean.FALSE).spannerParse());
    assertNull(new BooleanParser(null).spannerParse());
  }
}
