// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.utils;

import static com.google.cloud.spanner.pgadapter.statements.CopyToStatement.COPY_BINARY_HEADER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Interval;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import com.google.cloud.spanner.pgadapter.statements.ExtendedQueryProtocolHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConverterTest {

  @Test
  public void testConvertToPGScalarTypesStreamDirectly() throws IOException {
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(output);

    // BOOL
    ResultSet boolRs = mock(ResultSet.class);
    when(boolRs.getColumnType(0)).thenReturn(Type.bool());
    when(boolRs.getBoolean(0)).thenReturn(true);
    assertNull(
        Converter.convertToPG(outputStream, boolRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(outputStream, boolRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // BYTES
    ResultSet bytesRs = mock(ResultSet.class);
    when(bytesRs.getColumnType(0)).thenReturn(Type.bytes());
    Value bytesVal = mock(Value.class);
    when(bytesVal.getAsString())
        .thenReturn(Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}));
    when(bytesRs.getValue(0)).thenReturn(bytesVal);
    assertNull(
        Converter.convertToPG(outputStream, bytesRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, bytesRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // DATE
    ResultSet dateRs = mock(ResultSet.class);
    when(dateRs.getColumnType(0)).thenReturn(Type.date());
    when(dateRs.getDate(0)).thenReturn(Date.fromYearMonthDay(2024, 1, 1));
    when(dateRs.getString(0)).thenReturn("2024-01-01");
    assertNull(
        Converter.convertToPG(outputStream, dateRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(outputStream, dateRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // FLOAT32
    ResultSet floatRs = mock(ResultSet.class);
    when(floatRs.getColumnType(0)).thenReturn(Type.float32());
    when(floatRs.getFloat(0)).thenReturn(3.14f);
    assertNull(
        Converter.convertToPG(outputStream, floatRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, floatRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // FLOAT64
    ResultSet doubleRs = mock(ResultSet.class);
    when(doubleRs.getColumnType(0)).thenReturn(Type.float64());
    when(doubleRs.getDouble(0)).thenReturn(3.14159);
    assertNull(
        Converter.convertToPG(outputStream, doubleRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, doubleRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // INT64
    ResultSet intRs = mock(ResultSet.class);
    when(intRs.getColumnType(0)).thenReturn(Type.int64());
    when(intRs.getLong(0)).thenReturn(123456L);
    assertNull(
        Converter.convertToPG(outputStream, intRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(outputStream, intRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // PG_NUMERIC
    ResultSet numericRs = mock(ResultSet.class);
    when(numericRs.getColumnType(0)).thenReturn(Type.pgNumeric());
    when(numericRs.getString(0)).thenReturn("123.456");
    assertNull(
        Converter.convertToPG(
            outputStream, numericRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, numericRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // STRING
    ResultSet stringRs = mock(ResultSet.class);
    when(stringRs.getColumnType(0)).thenReturn(Type.string());
    when(stringRs.getString(0)).thenReturn("test string");
    assertNull(
        Converter.convertToPG(outputStream, stringRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // UUID
    ResultSet uuidRs = mock(ResultSet.class);
    when(uuidRs.getColumnType(0)).thenReturn(Type.uuid());
    when(uuidRs.getUuid(0)).thenReturn(UUID.randomUUID());
    assertNull(
        Converter.convertToPG(outputStream, uuidRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(outputStream, uuidRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // TIMESTAMP
    ResultSet tsRs = mock(ResultSet.class);
    when(tsRs.getColumnType(0)).thenReturn(Type.timestamp());
    when(tsRs.getTimestamp(0)).thenReturn(Timestamp.now());
    assertNull(
        Converter.convertToPG(outputStream, tsRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(outputStream, tsRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // INTERVAL
    ResultSet intervalRs = mock(ResultSet.class);
    when(intervalRs.getColumnType(0)).thenReturn(Type.interval());
    when(intervalRs.getInterval(0))
        .thenReturn(Interval.fromMonthsDaysNanos(2, 5, BigInteger.valueOf(1000L)));
    assertNull(
        Converter.convertToPG(
            outputStream, intervalRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, intervalRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
    output.reset();

    // PG_JSONB
    ResultSet jsonbRs = mock(ResultSet.class);
    when(jsonbRs.getColumnType(0)).thenReturn(Type.pgJsonb());
    when(jsonbRs.getPgJsonb(0)).thenReturn("{\"key\": \"value\"}");
    assertNull(
        Converter.convertToPG(outputStream, jsonbRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState));
    assertTrue(output.size() > 0);
    output.reset();
    assertNull(
        Converter.convertToPG(
            outputStream, jsonbRs, 0, DataFormat.POSTGRESQL_BINARY, sessionState));
    assertTrue(output.size() > 0);
  }

  @Test
  public void testConvertToPGArrayReturnsByteArray() throws IOException {
    SessionState sessionState = mock(SessionState.class);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(output);

    ResultSet arrayRs = mock(ResultSet.class);
    when(arrayRs.getColumnType(0)).thenReturn(Type.array(Type.string()));
    when(arrayRs.getValue(0)).thenReturn(Value.stringArray(Arrays.asList("foo", "bar")));

    byte[] result =
        Converter.convertToPG(outputStream, arrayRs, 0, DataFormat.POSTGRESQL_TEXT, sessionState);
    assertNotNull(result);
    assertTrue(result.length > 0);
    assertEquals(0, output.size()); // Nothing written directly to stream for ARRAY yet
  }

  @Test
  public void testConvertResultSetRowToDataRowResponse() throws Exception {
    IntermediateStatement statement = mock(IntermediateStatement.class);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));
    when(statement.getConnectionHandler()).thenReturn(connectionHandler);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(backendConnection.getSessionState()).thenReturn(sessionState);

    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isBinaryFormat()).thenReturn(false);
    when(options.getTextFormat()).thenReturn(TextFormat.POSTGRESQL);
    when(statement.getResultFormatCode(anyInt())).thenReturn((short) 0);

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnCount()).thenReturn(4);

    // col 0: NULL string
    when(resultSet.getColumnType(0)).thenReturn(Type.string());
    when(resultSet.isNull(0)).thenReturn(true);

    // col 1: INT64 in text mode (42)
    when(resultSet.getColumnType(1)).thenReturn(Type.int64());
    when(resultSet.isNull(1)).thenReturn(false);
    when(resultSet.getLong(1)).thenReturn(42L);

    // col 2: BOOL in text mode (true -> 't')
    when(resultSet.getColumnType(2)).thenReturn(Type.bool());
    when(resultSet.isNull(2)).thenReturn(false);
    when(resultSet.getBoolean(2)).thenReturn(true);

    // col 3: ARRAY in text mode ({"hello","world"})
    when(resultSet.getColumnType(3)).thenReturn(Type.array(Type.string()));
    when(resultSet.isNull(3)).thenReturn(false);
    when(resultSet.getValue(3)).thenReturn(Value.stringArray(Arrays.asList("hello", "world")));

    try (Converter converter =
        new Converter(statement, QueryMode.EXTENDED, options, resultSet, false)) {
      int size = converter.convertResultSetRowToDataRowResponse();
      assertTrue(size > 0);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(out);
      converter.writeBuffer(dataOut);

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
      // 4 columns
      assertEquals(4, in.readShort());

      // col 0: NULL column writes -1 length
      assertEquals(-1, in.readInt());

      // col 1: INT64 text "42"
      int len1 = in.readInt();
      byte[] val1 = new byte[len1];
      in.readFully(val1);
      assertEquals("42", new String(val1, StandardCharsets.UTF_8));

      // col 2: BOOL text 't'
      int len2 = in.readInt();
      assertEquals(1, len2);
      assertEquals('t', in.readByte());

      // col 3: ARRAY text '{"hello","world"}'
      int len3 = in.readInt();
      byte[] val3 = new byte[len3];
      in.readFully(val3);
      assertEquals("{\"hello\",\"world\"}", new String(val3, StandardCharsets.UTF_8));
      assertEquals(0, in.available());
    }
  }

  @Test
  public void testBinaryCopyHeaderInFirstRowOnly() throws Exception {
    IntermediateStatement statement = mock(IntermediateStatement.class);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(statement.getConnectionHandler()).thenReturn(connectionHandler);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(backendConnection.getSessionState()).thenReturn(sessionState);

    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isBinaryFormat()).thenReturn(false);
    when(options.getTextFormat()).thenReturn(TextFormat.POSTGRESQL);
    when(statement.getResultFormatCode(anyInt())).thenReturn((short) 0);

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnCount()).thenReturn(1);
    when(resultSet.getColumnType(0)).thenReturn(Type.int64());
    when(resultSet.isNull(0)).thenReturn(false);
    when(resultSet.getLong(0)).thenReturn(100L);

    try (Converter converter =
        new Converter(statement, QueryMode.EXTENDED, options, resultSet, true)) {
      assertTrue(converter.isIncludeBinaryCopyHeaderInFirstRow());

      // Row 1 includes copy binary header
      converter.convertResultSetRowToDataRowResponse();
      ByteArrayOutputStream out1 = new ByteArrayOutputStream();
      converter.writeBuffer(new DataOutputStream(out1));
      byte[] bytes1 = out1.toByteArray();
      assertTrue(bytes1.length >= COPY_BINARY_HEADER.length + 8);
      byte[] header = new byte[COPY_BINARY_HEADER.length];
      System.arraycopy(bytes1, 0, header, 0, header.length);
      assertArrayEquals(COPY_BINARY_HEADER, header);

      // Row 2 does NOT include copy binary header
      converter.convertResultSetRowToDataRowResponse();
      ByteArrayOutputStream out2 = new ByteArrayOutputStream();
      converter.writeBuffer(new DataOutputStream(out2));
      byte[] bytes2 = out2.toByteArray();
      assertTrue(bytes2.length < bytes1.length);
      DataInputStream in2 = new DataInputStream(new ByteArrayInputStream(bytes2));
      assertEquals(1, in2.readShort()); // Starts immediately with column count
    }
  }

  @Test
  public void testConvertResultSetRowToDataRowResponseWithBinaryColumns() throws Exception {
    IntermediateStatement statement = mock(IntermediateStatement.class);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ExtendedQueryProtocolHandler extendedQueryProtocolHandler =
        mock(ExtendedQueryProtocolHandler.class);
    BackendConnection backendConnection = mock(BackendConnection.class);
    SessionState sessionState = mock(SessionState.class);
    when(sessionState.getTimezone()).thenReturn(ZoneId.of("UTC"));
    when(statement.getConnectionHandler()).thenReturn(connectionHandler);
    when(connectionHandler.getExtendedQueryProtocolHandler())
        .thenReturn(extendedQueryProtocolHandler);
    when(extendedQueryProtocolHandler.getBackendConnection()).thenReturn(backendConnection);
    when(backendConnection.getSessionState()).thenReturn(sessionState);

    OptionsMetadata options = mock(OptionsMetadata.class);
    when(options.isBinaryFormat()).thenReturn(false);
    when(options.getTextFormat()).thenReturn(TextFormat.POSTGRESQL);
    // Format code 1 = BINARY
    when(statement.getResultFormatCode(anyInt())).thenReturn((short) 1);

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getColumnCount()).thenReturn(3);

    // col 0: INT64 in binary mode
    when(resultSet.getColumnType(0)).thenReturn(Type.int64());
    when(resultSet.isNull(0)).thenReturn(false);
    when(resultSet.getLong(0)).thenReturn(100L);

    // col 1: BOOL in binary mode
    when(resultSet.getColumnType(1)).thenReturn(Type.bool());
    when(resultSet.isNull(1)).thenReturn(false);
    when(resultSet.getBoolean(1)).thenReturn(true);

    // col 2: NULL column
    when(resultSet.getColumnType(2)).thenReturn(Type.float64());
    when(resultSet.isNull(2)).thenReturn(true);

    try (Converter converter =
        new Converter(statement, QueryMode.EXTENDED, options, resultSet, false)) {
      int size = converter.convertResultSetRowToDataRowResponse();
      assertTrue(size > 0);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      converter.writeBuffer(new DataOutputStream(out));

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
      assertEquals(3, in.readShort()); // 3 columns

      // col 0: INT64 binary length 8, value 100L
      assertEquals(8, in.readInt());
      assertEquals(100L, in.readLong());

      // col 1: BOOL binary length 1, value 1
      assertEquals(1, in.readInt());
      assertEquals(1, in.readByte());

      // col 2: NULL column length -1
      assertEquals(-1, in.readInt());
      assertEquals(0, in.available());
    }
  }
}
