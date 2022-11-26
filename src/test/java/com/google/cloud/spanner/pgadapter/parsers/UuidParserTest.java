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

import static com.google.cloud.spanner.pgadapter.parsers.UuidParser.binaryEncode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class UuidParserTest {

  @Test
  public void testCreate() {
    assertEquals(
        UuidParser.class,
        Parser.create(
                mock(SessionState.class),
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8),
                Oid.UUID,
                FormatCode.TEXT)
            .getClass());
    assertEquals(
        UuidParser.class,
        Parser.create(
                mock(SessionState.class),
                binaryEncode(UUID.randomUUID().toString()),
                Oid.UUID,
                FormatCode.BINARY)
            .getClass());
  }

  @Test
  public void testTextToText() {
    String uuidStringValue = "c852ee2a-7521-4a70-a02f-2b9d0dd9c19a";
    UuidParser parser =
        new UuidParser(uuidStringValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT);
    assertEquals(uuidStringValue, parser.stringParse());

    parser = new UuidParser(null, FormatCode.TEXT);
    assertNull(parser.stringParse());
  }

  @Test
  public void testTextToBinary() {
    String uuidStringValue = "c852ee2a-7521-4a70-a02f-2b9d0dd9c19a";
    UuidParser parser =
        new UuidParser(uuidStringValue.getBytes(StandardCharsets.UTF_8), FormatCode.TEXT);
    UUID uuid = UUID.fromString(uuidStringValue);
    byte[] bytes = new byte[16];
    ByteConverter.int8(bytes, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(bytes, 8, uuid.getLeastSignificantBits());
    assertArrayEquals(bytes, parser.binaryParse());

    parser = new UuidParser(null, FormatCode.TEXT);
    assertNull(parser.binaryParse());
  }

  @Test
  public void testBinaryToText() {
    String uuidStringValue = "c852ee2a-7521-4a70-a02f-2b9d0dd9c19a";
    UUID uuid = UUID.fromString(uuidStringValue);
    byte[] bytes = new byte[16];
    ByteConverter.int8(bytes, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(bytes, 8, uuid.getLeastSignificantBits());

    UuidParser parser = new UuidParser(bytes, FormatCode.BINARY);
    assertEquals(uuidStringValue, parser.stringParse());

    parser = new UuidParser(null, FormatCode.BINARY);
    assertNull(parser.stringParse());
  }

  @Test
  public void testBinaryToBinary() {
    String uuidStringValue = "c852ee2a-7521-4a70-a02f-2b9d0dd9c19a";
    UUID uuid = UUID.fromString(uuidStringValue);
    byte[] bytes = new byte[16];
    ByteConverter.int8(bytes, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(bytes, 8, uuid.getLeastSignificantBits());

    UuidParser parser = new UuidParser(bytes, FormatCode.BINARY);
    assertArrayEquals(bytes, parser.binaryParse());

    parser = new UuidParser(null, FormatCode.BINARY);
    assertNull(parser.binaryParse());
  }

  @Test
  public void testInvalidBinaryInput() {
    PGException exception =
        assertThrows(PGException.class, () -> new UuidParser(new byte[8], FormatCode.BINARY));
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
    assertEquals(Severity.ERROR, exception.getSeverity());
  }

  @Test
  public void testInvalidTextInput() {
    PGException exception =
        assertThrows(
            PGException.class,
            () -> new UuidParser("foo".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
    assertEquals(Severity.ERROR, exception.getSeverity());
  }
}
