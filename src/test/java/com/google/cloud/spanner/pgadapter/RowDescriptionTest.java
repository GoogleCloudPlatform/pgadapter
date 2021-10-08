package com.google.cloud.spanner.pgadapter.wireoutput;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

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
import java.sql.ResultSetMetaData;
import java.sql.Types;
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
  @Mock private ResultSetMetaData metadata;

  @Before
  public void setUp() {}

  @Test
  public void OidTest() throws Exception {
    Mockito.when(metadata.getColumnCount()).thenReturn(0);
    Mockito.when(metadata.getColumnType(1)).thenReturn(Types.SMALLINT);
    Mockito.when(metadata.getColumnType(2)).thenReturn(Types.INTEGER);
    Mockito.when(metadata.getColumnType(3)).thenReturn(Types.BIGINT);
    Mockito.when(metadata.getColumnType(4)).thenReturn(Types.NUMERIC);
    Mockito.when(metadata.getColumnType(5)).thenReturn(Types.REAL);
    Mockito.when(metadata.getColumnType(6)).thenReturn(Types.DOUBLE);
    Mockito.when(metadata.getColumnType(7)).thenReturn(Types.CHAR);
    Mockito.when(metadata.getColumnType(8)).thenReturn(Types.VARCHAR);
    Mockito.when(metadata.getColumnType(9)).thenReturn(Types.BINARY);
    Mockito.when(metadata.getColumnType(10)).thenReturn(Types.BIT);
    Mockito.when(metadata.getColumnType(11)).thenReturn(Types.DATE);
    Mockito.when(metadata.getColumnType(12)).thenReturn(Types.TIME);
    Mockito.when(metadata.getColumnType(13)).thenReturn(Types.TIMESTAMP);

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
            commandMetadata);
    QueryMode mode = QueryMode.SIMPLE;
    RowDescriptionResponse response =
        new RowDescriptionResponse(output, statement, metadata, options, mode);

    // Types.SMALLINT
    Assert.assertEquals(response.getOidType(1), Oid.INT2);
    Assert.assertEquals(response.getOidTypeSize(Oid.INT2), 2);
    // Types.INTEGER
    Assert.assertEquals(response.getOidType(2), Oid.INT4);
    Assert.assertEquals(response.getOidTypeSize(Oid.INT4), 4);
    // Types.BIGINT
    Assert.assertEquals(response.getOidType(3), Oid.INT8);
    Assert.assertEquals(response.getOidTypeSize(Oid.INT8), 8);
    // Types.NUMERIC
    Assert.assertEquals(response.getOidType(4), Oid.NUMERIC);
    Assert.assertEquals(response.getOidTypeSize(Oid.NUMERIC), -1);
    // Types.REAL
    Assert.assertEquals(response.getOidType(5), Oid.FLOAT4);
    Assert.assertEquals(response.getOidTypeSize(Oid.FLOAT4), 4);
    // Types.DOUBLE
    Assert.assertEquals(response.getOidType(6), Oid.FLOAT8);
    Assert.assertEquals(response.getOidTypeSize(Oid.FLOAT8), 8);
    // Types.CHAR
    Assert.assertEquals(response.getOidType(7), Oid.CHAR);
    Assert.assertEquals(response.getOidTypeSize(Oid.CHAR), 1);
    // Types.VARCHAR
    Assert.assertEquals(response.getOidType(8), Oid.VARCHAR);
    Assert.assertEquals(response.getOidTypeSize(Oid.VARCHAR), -1);
    // Types.BINARY
    Assert.assertEquals(response.getOidType(9), Oid.BYTEA);
    Assert.assertEquals(response.getOidTypeSize(Oid.BYTEA), -1);
    // Types.BIT
    Assert.assertEquals(response.getOidType(10), Oid.BOOL);
    Assert.assertEquals(response.getOidTypeSize(Oid.BOOL), 1);
    // Types.DATE
    Assert.assertEquals(response.getOidType(11), Oid.DATE);
    Assert.assertEquals(response.getOidTypeSize(Oid.DATE), 8);
    // Types.TIME
    Assert.assertEquals(response.getOidType(12), Oid.TIME);
    Assert.assertEquals(response.getOidTypeSize(Oid.TIME), 8);
    // Types.TIMESTAMP
    Assert.assertEquals(response.getOidType(13), Oid.TIMESTAMP);
    Assert.assertEquals(response.getOidTypeSize(Oid.TIMESTAMP), 12);
  }

  @Test
  public void SendPayloadNullStatementTest() throws Exception {
    int COLUMN_COUNT = 1;
    String COLUMN_NAME = "default-column-name";
    Mockito.when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    Mockito.when(metadata.getColumnName(Mockito.anyInt())).thenReturn(COLUMN_NAME);
    Mockito.when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Types.SMALLINT);
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
    Assert.assertEquals(outputReader.readInt(), Oid.INT2);
    // type size
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 2)));
    // type modifier
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // format code
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 0)));
  }

  @Test
  public void SendPayloadStatementWithBinaryOptionTest() throws Exception {
    int COLUMN_COUNT = 1;
    String COLUMN_NAME = "default-column-name";
    Mockito.when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    Mockito.when(metadata.getColumnName(Mockito.anyInt())).thenReturn(COLUMN_NAME);
    Mockito.when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Types.SMALLINT);
    Mockito.when(statement.getResultFormatCode(Mockito.anyInt())).thenReturn((short) 0);
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
    Assert.assertEquals(outputReader.readInt(), Oid.INT2);
    // type size
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 2)));
    // type modifier
    Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
    // format code
    Assert.assertThat(outputReader.readShort(), is(equalTo((short) 1)));
  }

  @Test
  public void SendPayloadStatementTest() throws Exception {
    int COLUMN_COUNT = 2;
    String COLUMN_NAME = "default-column-name";
    Mockito.when(metadata.getColumnCount()).thenReturn(COLUMN_COUNT);
    Mockito.when(metadata.getColumnName(Mockito.anyInt())).thenReturn(COLUMN_NAME);
    Mockito.when(metadata.getColumnType(Mockito.anyInt())).thenReturn(Types.SMALLINT);
    Mockito.when(statement.getResultFormatCode(Mockito.anyInt()))
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
      Assert.assertEquals(outputReader.readInt(), Oid.INT2);
      // type size
      Assert.assertThat(outputReader.readShort(), is(equalTo((short) 2)));
      // type modifier
      Assert.assertThat(outputReader.readInt(), is(equalTo((int) DEFAULT_FLAG)));
      // format code
      Assert.assertThat(outputReader.readShort(), is(equalTo((short) i)));
    }
  }
}
