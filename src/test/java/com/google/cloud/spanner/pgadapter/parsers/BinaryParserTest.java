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

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Utils;

@RunWith(JUnit4.class)
public class BinaryParserTest {

  @Test
  public void testToByteArray() {
    assertEquals(
        ByteArray.copyFrom("test"),
        BinaryParser.toByteArray("test".getBytes(StandardCharsets.UTF_8)));
    assertEquals(ByteArray.copyFrom(new byte[] {}), BinaryParser.toByteArray(new byte[] {}));
  }

  @Test
  public void testStringParse() {
    assertEquals(
        "\\x010203", new BinaryParser(ByteArray.copyFrom(new byte[] {1, 2, 3})).stringParse());
    assertNull(new BinaryParser(null).stringParse());
    assertThrows(
        PGException.class,
        () -> new BinaryParser("\\xzz".getBytes(StandardCharsets.UTF_8), FormatCode.TEXT));
  }

  @Test
  public void testBytesToHex() {
    Random random = new Random();
    byte[] value = new byte[random.nextInt(1024) + 1];
    random.nextBytes(value);
    assertEquals("\\x" + Utils.toHexString(value), new String(BinaryParser.bytesToHex(value)));
  }

  @Test
  public void testConvertToPG() throws IOException {
    byte[] rawBytes = new byte[] {1, 2, 3, 4};
    ResultSet resultSet = mock(ResultSet.class);
    Value value = mock(Value.class);
    when(value.getAsString()).thenReturn(Base64.getEncoder().encodeToString(rawBytes));
    when(resultSet.getValue(0)).thenReturn(value);

    SessionState sessionState = mock(SessionState.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);

    // Text format
    assertNull(
        BinaryParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_TEXT));
    assertArrayEquals(
        new byte[] {0, 0, 0, 10, '\\', 'x', '0', '1', '0', '2', '0', '3', '0', '4'},
        output.toByteArray());
    output.reset();

    // Binary format
    assertNull(
        BinaryParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(new byte[] {0, 0, 0, 4, 1, 2, 3, 4}, output.toByteArray());
  }

  @Test
  public void testConvertToPGProtobufResultSet() throws IOException {
    byte[] rawBytes = new byte[] {5, 6, 7, 8};
    String base64 = Base64.getEncoder().encodeToString(rawBytes);
    ProtobufResultSet resultSet = mock(ProtobufResultSet.class);
    when(resultSet.canGetProtobufValue(0)).thenReturn(true);
    com.google.protobuf.Value protoValue =
        com.google.protobuf.Value.newBuilder().setStringValue(base64).build();
    when(resultSet.getProtobufValue(0)).thenReturn(protoValue);

    SessionState sessionState = mock(SessionState.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(output);

    assertNull(
        BinaryParser.convertToPG(
            sessionState, dataOutputStream, resultSet, 0, DataFormat.POSTGRESQL_BINARY));
    assertArrayEquals(new byte[] {0, 0, 0, 4, 5, 6, 7, 8}, output.toByteArray());
  }
}
