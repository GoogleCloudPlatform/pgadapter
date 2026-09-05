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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    assertTrue(BooleanParser.toBoolean("TRUE"));
    assertTrue(BooleanParser.toBoolean("TRU"));
    assertTrue(BooleanParser.toBoolean("TR"));
    assertTrue(BooleanParser.toBoolean("T"));

    assertTrue(BooleanParser.toBoolean("on"));
    assertTrue(BooleanParser.toBoolean("On"));
    assertTrue(BooleanParser.toBoolean("ON"));

    assertFalse(BooleanParser.toBoolean("false"));
    assertFalse(BooleanParser.toBoolean("fals"));
    assertFalse(BooleanParser.toBoolean("fal"));
    assertFalse(BooleanParser.toBoolean("fa"));
    assertFalse(BooleanParser.toBoolean("f"));

    assertFalse(BooleanParser.toBoolean("FALSE"));
    assertFalse(BooleanParser.toBoolean("FALS"));
    assertFalse(BooleanParser.toBoolean("FAL"));
    assertFalse(BooleanParser.toBoolean("FA"));
    assertFalse(BooleanParser.toBoolean("F"));

    assertFalse(BooleanParser.toBoolean("off"));
    assertFalse(BooleanParser.toBoolean("OFF"));
    assertFalse(BooleanParser.toBoolean("Off"));
    assertFalse(BooleanParser.toBoolean("Of"));
    assertFalse(BooleanParser.toBoolean("OF"));
    assertFalse(BooleanParser.toBoolean("of"));

    assertTrue(BooleanParser.toBoolean("1"));
    assertFalse(BooleanParser.toBoolean("0"));

    assertTrue(BooleanParser.toBoolean("yes"));
    assertTrue(BooleanParser.toBoolean("ye"));
    assertTrue(BooleanParser.toBoolean("y"));
    assertFalse(BooleanParser.toBoolean("no"));
    assertFalse(BooleanParser.toBoolean("n"));
    assertTrue(BooleanParser.toBoolean("Yes"));
    assertFalse(BooleanParser.toBoolean("nO"));
    assertTrue(BooleanParser.toBoolean("YES"));
    assertFalse(BooleanParser.toBoolean("NO"));

    assertThrows(PGException.class, () -> BooleanParser.toBoolean("foo"));
    assertThrows(
        PGException.class,
        () -> new BooleanParser("bar".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
    assertThrows(PGException.class, () -> BooleanParser.toBoolean("2"));
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

  @Test
  public void testConvertToPG() throws IOException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getBoolean(0)).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(false);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    // Text format
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(new byte[] {0, 0, 0, 1, 't'}, output.toByteArray());
    output.reset();
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 1, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(new byte[] {0, 0, 0, 1, 'f'}, output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(new byte[] {0, 0, 0, 1, 1}, output.toByteArray());
    output.reset();
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 1, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(new byte[] {0, 0, 0, 1, 0}, output.toByteArray());
    output.reset();

    // Spanner format
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.SPANNER));
    assertArrayEquals(new byte[] {0, 0, 0, 4, 't', 'r', 'u', 'e'}, output.toByteArray());
    output.reset();
    assertNull(
        BooleanParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 1, DataFormat.SPANNER));
    assertArrayEquals(new byte[] {0, 0, 0, 5, 'f', 'a', 'l', 's', 'e'}, output.toByteArray());
    output.reset();

    // Verify backward compatibility method
    assertArrayEquals(
        new byte[] {'t'}, BooleanParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(
        new byte[] {'f'}, BooleanParser.convertToPG(resultSet, 1, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(
        new byte[] {1}, BooleanParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(
        new byte[] {0}, BooleanParser.convertToPG(resultSet, 1, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(
        "true".getBytes(StandardCharsets.UTF_8),
        BooleanParser.convertToPG(resultSet, 0, DataFormat.SPANNER));
    assertArrayEquals(
        "false".getBytes(StandardCharsets.UTF_8),
        BooleanParser.convertToPG(resultSet, 1, DataFormat.SPANNER));
  }
}
