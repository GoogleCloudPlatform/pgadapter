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
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

  @Test
  public void testInvalidTextValueForBinaryEncode() {
    PGException exception = assertThrows(PGException.class, () -> UuidParser.binaryEncode("bar"));
    assertEquals(SQLState.InvalidParameterValue, exception.getSQLState());
    assertEquals(Severity.ERROR, exception.getSeverity());
  }

  @Test
  public void testHandleInvalidFormatCode() {
    PGException exception =
        assertThrows(PGException.class, () -> UuidParser.handleInvalidFormat(FormatCode.TEXT));
    assertEquals(SQLState.InternalError, exception.getSQLState());
    assertEquals(Severity.ERROR, exception.getSeverity());
    assertEquals("Unsupported format: TEXT", exception.getMessage());
  }

  @Test
  public void testConvertToPG() throws IOException {
    UUID uuid = UUID.fromString("c852ee2a-7521-4a70-a02f-2b9d0dd9c19a");
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getUuid(0)).thenReturn(uuid);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    // Text format
    assertNull(
        UuidParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] textBytes = uuid.toString().getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream expectedText = new ByteArrayOutputStream();
    DataOutputStream expectedTextStream = new DataOutputStream(expectedText);
    expectedTextStream.writeInt(textBytes.length);
    expectedTextStream.write(textBytes);
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        UuidParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    ByteArrayOutputStream expectedBinary = new ByteArrayOutputStream();
    DataOutputStream expectedBinaryStream = new DataOutputStream(expectedBinary);
    expectedBinaryStream.writeInt(16);
    expectedBinaryStream.writeLong(uuid.getMostSignificantBits());
    expectedBinaryStream.writeLong(uuid.getLeastSignificantBits());
    assertArrayEquals(expectedBinary.toByteArray(), output.toByteArray());
    output.reset();

    // Spanner format
    assertNull(
        UuidParser.convertToPG(sessionState, dataOutputStream, resultSet, 0, DataFormat.SPANNER));
    assertArrayEquals(expectedText.toByteArray(), output.toByteArray());
    output.reset();

    // Verify backward compatibility method
    assertArrayEquals(textBytes, UuidParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    byte[] binaryBytes = new byte[16];
    ByteConverter.int8(binaryBytes, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(binaryBytes, 8, uuid.getLeastSignificantBits());
    assertArrayEquals(
        binaryBytes, UuidParser.convertToPG(resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(textBytes, UuidParser.convertToPG(resultSet, 0, DataFormat.SPANNER));
  }

  @Test
  public void testConvertToPGNilUuid() throws IOException {
    UUID nilUuid = new UUID(0L, 0L);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getUuid(0)).thenReturn(nilUuid);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    assertNull(
        UuidParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(
        new byte[] {0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        output.toByteArray());
  }

  @Test
  public void testConvertToPGProtobufResultSet() throws IOException {
    String uuidString = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    ProtobufResultSet protobufResultSet = mock(ProtobufResultSet.class);
    when(protobufResultSet.canGetProtobufValue(0)).thenReturn(true);
    when(protobufResultSet.getProtobufValue(0))
        .thenReturn(com.google.protobuf.Value.newBuilder().setStringValue(uuidString).build());

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);
    SessionState sessionState = mock(SessionState.class);

    assertNull(
        UuidParser.convertToPG(
            sessionState, dataOutputStream, protobufResultSet, 0, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(
        new byte[] {
          0, 0, 0, 36, 'a', '0', 'e', 'e', 'b', 'c', '9', '9', '-', '9', 'c', '0', 'b', '-', '4',
          'e', 'f', '8', '-', 'b', 'b', '6', 'd', '-', '6', 'b', 'b', '9', 'b', 'd', '3', '8', '0',
          'a', '1', '1'
        },
        output.toByteArray());
  }
}
