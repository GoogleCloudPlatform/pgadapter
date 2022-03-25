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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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
import org.junit.Assert;
import org.junit.Before;
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
  private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private DataOutputStream output = new DataOutputStream(buffer);
  @Mock private IntermediateStatement statement;
  @Mock private ResultSet metadata;
  private Type rowType;

  @Before
  public void setUp() {
    rowType =
        Type.struct(
            StructField.of("default-column-name", Type.int64()),
            StructField.of("default-column-name", Type.int64()));
    when(metadata.getType()).thenReturn(rowType);
  }

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
    Assert.assertEquals(response.getOidType(0), Oid.INT8);
    Assert.assertEquals(response.getOidTypeSize(Oid.INT8), 8);
    // Types.NUMERIC
    Assert.assertEquals(response.getOidType(1), Oid.NUMERIC);
    Assert.assertEquals(response.getOidTypeSize(Oid.NUMERIC), -1);
    // Types.DOUBLE
    Assert.assertEquals(response.getOidType(2), Oid.FLOAT8);
    Assert.assertEquals(response.getOidTypeSize(Oid.FLOAT8), 8);
    // Types.VARCHAR
    Assert.assertEquals(response.getOidType(3), Oid.VARCHAR);
    Assert.assertEquals(response.getOidTypeSize(Oid.VARCHAR), -1);
    // Types.BINARY
    Assert.assertEquals(response.getOidType(4), Oid.BYTEA);
    Assert.assertEquals(response.getOidTypeSize(Oid.BYTEA), -1);
    // Types.BIT
    Assert.assertEquals(response.getOidType(5), Oid.BOOL);
    Assert.assertEquals(response.getOidTypeSize(Oid.BOOL), 1);
    // Types.DATE
    Assert.assertEquals(response.getOidType(6), Oid.DATE);
    Assert.assertEquals(response.getOidTypeSize(Oid.DATE), 8);
    // Types.TIMESTAMP
    Assert.assertEquals(response.getOidType(7), Oid.TIMESTAMPTZ);
    Assert.assertEquals(response.getOidTypeSize(Oid.TIMESTAMPTZ), 12);
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
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) COLUMN_COUNT)));
    // column name
    int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    outputReader.read(bytes, 0, numOfBytes);
    Assert.assertEquals(new String(bytes, UTF8), COLUMN_NAME);
    // null terminator
    Assert.assertThat(outputReader.readByte(), is(equalTo(DEFAULT_FLAG)));
    // table oid
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // column index
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) DEFAULT_FLAG)));
    // type oid
    Assert.assertEquals(outputReader.readInt(), Oid.INT8);
    // type size
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 8)));
    // type modifier
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // format code
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 0)));
  }

  @Test
  public void SendPayloadStatementWithBinaryOptionTest() throws Exception {
    int COLUMN_COUNT = 1;
    String COLUMN_NAME = "default-column-name";
    Type rowType = Type.struct(StructField.of(COLUMN_NAME, Type.string()));
    when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    when(metadata.getType()).thenReturn(rowType);
    when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Type.int64());
    when(statement.getResultFormatCode(Mockito.anyInt())).thenReturn((short) 0);
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
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) COLUMN_COUNT)));
    // column name
    int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    outputReader.read(bytes, 0, numOfBytes);
    Assert.assertEquals(new String(bytes, UTF8), COLUMN_NAME);
    // null terminator
    Assert.assertThat(outputReader.readByte(), is(equalTo(DEFAULT_FLAG)));
    // table oid
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // column index
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) DEFAULT_FLAG)));
    // type oid
    Assert.assertEquals(outputReader.readInt(), Oid.INT8);
    // type size
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 8)));
    // type modifier
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // format code
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 1)));
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
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) COLUMN_COUNT)));
    for (int i = 0; i < COLUMN_COUNT; i++) {
      // column name
      int numOfBytes = COLUMN_NAME.getBytes(UTF8).length;
      byte[] bytes = new byte[numOfBytes];
      outputReader.read(bytes, 0, numOfBytes);
      Assert.assertEquals(new String(bytes, UTF8), COLUMN_NAME);
      // null terminator
      Assert.assertThat(outputReader.readByte(), is(equalTo(DEFAULT_FLAG)));
      // table oid
      Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
      // column index
      Assert.assertThat(outputReader.readShort(), is(equalTo((short) DEFAULT_FLAG)));
      // type oid
      Assert.assertEquals(outputReader.readInt(), Oid.INT8);
      // type size
      Assert.assertThat(outputReader.readShort(), is(equalTo((short) 8)));
      // type modifier
      Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
      // format code
      Assert.assertThat(outputReader.readShort(), is(equalTo((short) i)));
    }
  }
}
