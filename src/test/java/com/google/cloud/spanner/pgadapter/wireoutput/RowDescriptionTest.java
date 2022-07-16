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

package com.google.cloud.spanner.pgadapter.wireoutput;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public final class RowDescriptionTest {

  private static final String EMPTY_COMMAND_JSON = "{\"commands\":[]}";
  private static final byte DEFAULT_FLAG = 0;
  private static final Charset UTF8 = StandardCharsets.UTF_8;

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private final DataOutputStream output = new DataOutputStream(buffer);
  @Mock private IntermediateStatement statement;
  @Mock private ResultSet metadata;

  @Test
  public void OidTest() throws Exception {
    when(metadata.getColumnCount()).thenReturn(0);
    when(metadata.getColumnType(0)).thenReturn(Type.int64());
    when(metadata.getColumnType(1)).thenReturn(Type.pgNumeric());
    when(metadata.getColumnType(2)).thenReturn(Type.float64());
    when(metadata.getColumnType(3)).thenReturn(Type.string());
    when(metadata.getColumnType(4)).thenReturn(Type.bytes());
    when(metadata.getColumnType(5)).thenReturn(Type.bool());
    when(metadata.getColumnType(6)).thenReturn(Type.date());
    when(metadata.getColumnType(7)).thenReturn(Type.timestamp());

    JSONParser parser = new JSONParser();
    JSONObject commandMetadata = (JSONObject) parser.parse(EMPTY_COMMAND_JSON);
    OptionsMetadata options =
        new OptionsMetadata(
            "jdbc:cloudspanner:/projects/test-project/instances/test-instance/databases/test-database",
            8888,
            TextFormat.POSTGRESQL,
            false,
            false,
            false,
            false,
            commandMetadata);
    QueryMode mode = QueryMode.SIMPLE;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, statement, metadata, options, mode);

    // Types.BIGINT
    assertEquals(Oid.INT8, response.getOidType(0));
    assertEquals(8, response.getOidTypeSize(Oid.INT8));
    // Types.NUMERIC
    assertEquals(Oid.NUMERIC, response.getOidType(1));
    assertEquals(-1, response.getOidTypeSize(Oid.NUMERIC));
    // Types.DOUBLE
    assertEquals(Oid.FLOAT8, response.getOidType(2));
    assertEquals(8, response.getOidTypeSize(Oid.FLOAT8));
    // Types.VARCHAR
    assertEquals(Oid.VARCHAR, response.getOidType(3));
    assertEquals(-1, response.getOidTypeSize(Oid.VARCHAR));
    // Types.BINARY
    assertEquals(Oid.BYTEA, response.getOidType(4));
    assertEquals(-1, response.getOidTypeSize(Oid.BYTEA));
    // Types.BIT
    assertEquals(Oid.BOOL, response.getOidType(5));
    assertEquals(1, response.getOidTypeSize(Oid.BOOL));
    // Types.DATE
    assertEquals(Oid.DATE, response.getOidType(6));
    assertEquals(8, response.getOidTypeSize(Oid.DATE));
    // Types.TIMESTAMP
    assertEquals(Oid.TIMESTAMPTZ, response.getOidType(7));
    assertEquals(12, response.getOidTypeSize(Oid.TIMESTAMPTZ));
  }

  @Test
  public void SendPayloadNullStatementTest() throws Exception {
    int COLUMN_COUNT = 1;
    String COLUMN_NAME = "default-column-name";
    Type rowType = Type.struct(StructField.of(COLUMN_NAME, Type.string()));
    when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    when(metadata.getType()).thenReturn(rowType);
    when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Type.int64());

    JSONParser parser = new JSONParser();
    JSONObject commandMetadata = (JSONObject) parser.parse(EMPTY_COMMAND_JSON);
    OptionsMetadata options =
        new OptionsMetadata(
            "jdbc:cloudspanner:/projects/test-project/instances/test-instance/databases/test-database",
            8888,
            TextFormat.POSTGRESQL,
            false,
            false,
            false,
            false,
            commandMetadata);
    QueryMode mode = QueryMode.EXTENDED;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, null, metadata, options, mode);
    response.sendPayload();
    DataInputStream outputReader =
        new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

    // column count
    assertEquals(COLUMN_COUNT, outputReader.readShort());
    // column name
    int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    assertEquals(numOfBytes, outputReader.read(bytes, 0, numOfBytes));
    assertEquals(new String(bytes, UTF8), COLUMN_NAME);
    // null terminator
    assertEquals(DEFAULT_FLAG, outputReader.readByte());
    // table oid
    assertEquals(DEFAULT_FLAG, outputReader.readInt());
    // column index
    assertEquals(DEFAULT_FLAG, outputReader.readShort());
    // type oid
    assertEquals(Oid.INT8, outputReader.readInt());
    // type size
    assertEquals(8, outputReader.readShort());
    // type modifier
    assertEquals(-1, outputReader.readInt());
    // format code
    assertEquals(0, outputReader.readShort());
  }

  @Test
  public void SendPayloadStatementWithBinarayAndTextOptionTest() throws Exception {
    Type rowType =
        Type.struct(
            StructField.of("numeric-text", Type.pgNumeric()),
            StructField.of("numeric-binary", Type.pgNumeric()));
    when(metadata.getColumnCount()).thenReturn(rowType.getStructFields().size());
    when(metadata.getType()).thenReturn(rowType);
    when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Type.pgNumeric());
    when(statement.getResultFormatCode(0)).thenReturn((short) 0);
    when(statement.getResultFormatCode(1)).thenReturn((short) 1);
    JSONParser parser = new JSONParser();
    JSONObject commandMetadata = (JSONObject) parser.parse(EMPTY_COMMAND_JSON);
    OptionsMetadata options =
        new OptionsMetadata(
            "jdbc:cloudspanner:/projects/test-project/instances/test-instance/databases/test-database",
            8888,
            TextFormat.POSTGRESQL,
            true,
            false,
            false,
            false,
            commandMetadata);
    QueryMode mode = QueryMode.EXTENDED;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, statement, metadata, options, mode);
    response.sendPayload();
    DataInputStream outputReader =
        new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

    // column count
    assertEquals(2, outputReader.readShort());
    // column name
    int numOfBytes = "numeric-text".getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    assertEquals(numOfBytes, outputReader.read(bytes, 0, numOfBytes));
    assertEquals("numeric-text", new String(bytes, UTF8));
    // null terminator
    assertEquals(DEFAULT_FLAG, outputReader.readByte());
    // table oid
    assertEquals(DEFAULT_FLAG, outputReader.readInt());
    // column index
    assertEquals(DEFAULT_FLAG, outputReader.readShort());
    // type oid
    assertEquals(Oid.NUMERIC, outputReader.readInt());
    // type size
    assertEquals(-1, outputReader.readShort());
    // type modifier
    assertEquals(-1, outputReader.readInt());
    // format code
    assertEquals(0, outputReader.readShort());

    // column name
    numOfBytes = "numeric-binary".getBytes(UTF8).length;
    bytes = new byte[numOfBytes];
    assertEquals(numOfBytes, outputReader.read(bytes, 0, numOfBytes));
    assertEquals("numeric-binary", new String(bytes, UTF8));
    // null terminator
    assertEquals(DEFAULT_FLAG, outputReader.readByte());
    // table oid
    assertEquals(DEFAULT_FLAG, outputReader.readInt());
    // column index
    assertEquals(DEFAULT_FLAG, outputReader.readShort());
    // type oid
    assertEquals(Oid.NUMERIC, outputReader.readInt());
    // type size
    assertEquals(-1, outputReader.readShort());
    // type modifier
    assertEquals(-1, outputReader.readInt());
    // format code
    assertEquals(1, outputReader.readShort());
  }

  @Test
  public void SendPayloadStatementWithBinaryOptionTest() throws Exception {
    int COLUMN_COUNT = 1;
    String COLUMN_NAME = "default-column-name";
    Type rowType = Type.struct(StructField.of(COLUMN_NAME, Type.string()));
    when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    when(metadata.getType()).thenReturn(rowType);
    when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Type.int64());
    when(statement.getResultFormatCode(Mockito.anyInt())).thenReturn((short) 1);
    JSONParser parser = new JSONParser();
    JSONObject commandMetadata = (JSONObject) parser.parse(EMPTY_COMMAND_JSON);
    OptionsMetadata options =
        new OptionsMetadata(
            "jdbc:cloudspanner:/projects/test-project/instances/test-instance/databases/test-database",
            8888,
            TextFormat.POSTGRESQL,
            true,
            false,
            false,
            false,
            commandMetadata);
    QueryMode mode = QueryMode.EXTENDED;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, statement, metadata, options, mode);
    response.sendPayload();
    DataInputStream outputReader =
        new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));

    // column count
    assertEquals(COLUMN_COUNT, outputReader.readShort());
    // column name
    int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    assertEquals(numOfBytes, outputReader.read(bytes, 0, numOfBytes));
    assertEquals(COLUMN_NAME, new String(bytes, UTF8));
    // null terminator
    assertEquals(DEFAULT_FLAG, outputReader.readByte());
    // table oid
    assertEquals(DEFAULT_FLAG, outputReader.readInt());
    // column index
    assertEquals(DEFAULT_FLAG, outputReader.readShort());
    // type oid
    assertEquals(Oid.INT8, outputReader.readInt());
    // type size
    assertEquals(8, outputReader.readShort());
    // type modifier
    assertEquals(-1, outputReader.readInt());
    // format code
    assertEquals(1, outputReader.readShort());
  }

  @Test
  public void SendPayloadStatementTest() throws Exception {
    int COLUMN_COUNT = 2;
    String COLUMN_NAME = "default-column-name";
    Type rowType =
        Type.struct(
            StructField.of(COLUMN_NAME, Type.string()), StructField.of(COLUMN_NAME, Type.string()));
    when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    when(metadata.getType()).thenReturn(rowType);
    when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Type.int64());
    when(statement.getResultFormatCode(Mockito.anyInt()))
        .thenReturn((short) 0)
        .thenReturn((short) 0)
        .thenReturn((short) 1);
    JSONParser parser = new JSONParser();
    JSONObject commandMetadata = (JSONObject) parser.parse(EMPTY_COMMAND_JSON);
    OptionsMetadata options =
        new OptionsMetadata(
            "jdbc:cloudspanner:/projects/test-project/instances/test-instance/databases/test-database",
            8888,
            TextFormat.POSTGRESQL,
            false,
            false,
            false,
            false,
            commandMetadata);
    QueryMode mode = QueryMode.EXTENDED;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, statement, metadata, options, mode);
    response.sendPayload();
    DataInputStream outputReader =
        new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));
    // column count
    assertEquals(COLUMN_COUNT, outputReader.readShort());
    for (int i = 0; i < COLUMN_COUNT; i++) {
      // column name
      int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
      byte[] bytes = new byte[numOfBytes];
      assertEquals(numOfBytes, outputReader.read(bytes, 0, numOfBytes));
      assertEquals(new String(bytes, UTF8), COLUMN_NAME);
      // null terminator
      assertEquals(DEFAULT_FLAG, outputReader.readByte());
      // table oid
      assertEquals(DEFAULT_FLAG, outputReader.readInt());
      // column index
      assertEquals(DEFAULT_FLAG, outputReader.readShort());
      // type oid
      assertEquals(Oid.INT8, outputReader.readInt());
      // type size
      assertEquals(8, outputReader.readShort());
      // type modifier
      assertEquals(-1, outputReader.readInt());
      // format code
      assertEquals(i, outputReader.readShort());
    }
  }
}
