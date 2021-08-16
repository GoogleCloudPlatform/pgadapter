package com.google.cloud.spanner.pgadapter.wireoutput;

import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.DataOutputStream;
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

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private DataOutputStream output;
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
}
