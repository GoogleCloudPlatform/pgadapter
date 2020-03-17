// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.PSQLStatement;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CloseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyDataMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyDoneMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyFailMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FlushMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FunctionCallMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.TerminateMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage.PreparedType;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.postgresql.util.ByteConverter;

@RunWith(JUnit4.class)
public class ProtocolTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();
  @Mock
  private ConnectionHandler connectionHandler;
  @Mock
  private Connection connection;
  @Mock
  private Statement statement;
  @Mock
  private ProxyServer server;
  @Mock
  private OptionsMetadata options;
  @Mock
  private IntermediatePreparedStatement intermediatePreparedStatement;
  @Mock
  private IntermediatePortalStatement intermediatePortalStatement;
  @Mock
  private DescribeStatementMetadata describeStatementMetadata;
  @Mock
  private DescribePortalMetadata describePortalMetadata;

  private byte[] intToBytes(int value) {
    byte[] parameters = new byte[4];
    ByteConverter.int4(parameters, 0, value);
    return parameters;
  }

  @Test
  public void testQueryMessage() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, 24};
    String payload = "SELECT * FROM users\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    String expectedSQL = "SELECT * FROM users";

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.isPSQLMode()).thenReturn(false);
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), QueryMessage.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getSql(), expectedSQL);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleQuery(((QueryMessage) message).getStatement());
    Mockito.verify(statement, Mockito.times(1)).execute(expectedSQL);
  }

  @Test
  public void testQueryUsesPSQLStatementWhenPSQLModeSelectedMessage() throws Exception {
    JSONParser parser = new JSONParser();
    byte[] messageMetadata = {'Q', 0, 0, 0, 24};
    String payload = "SELECT * FROM users\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    String expectedSQL = "SELECT * FROM users";

    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.isPSQLMode()).thenReturn(true);
    Mockito.when(options.getCommandMetadataJSON())
        .thenReturn((JSONObject) parser.parse("{\"commands\": []}"));
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), QueryMessage.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getClass(), PSQLStatement.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getSql(), expectedSQL);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleQuery(((QueryMessage) message).getStatement());
    Mockito.verify(statement, Mockito.times(1)).execute(expectedSQL);
  }

  @Test(expected = IOException.class)
  public void testQueryMessageFailsWhenNotNullTerminated() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, 23};
    String payload = "SELECT * FROM users";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.isPSQLMode()).thenReturn(false);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage.create(connectionHandler, inputStream);
  }

  @Test
  public void testParseMessage() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload = "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length = intToBytes(
        4
            + statementName.length()
            + payload.length()
            + parameterCount.length
            + parameters.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementName.getBytes(),
        payload.getBytes(),
        parameterCount,
        parameters
    );

    List<Integer> expectedParameterDataTypes = Arrays.asList(1002);
    String expectedSQL = "SELECT * FROM users WHERE name = ?";
    String expectedMessageName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), ParseMessage.class);
    Assert.assertEquals(((ParseMessage) message).getName(), expectedMessageName);
    Assert.assertEquals(
        ((ParseMessage) message).getStatement().getSql(),
        expectedSQL);
    Assert.assertThat(
        ((ParseMessage) message).getStatement().getParameterDataTypes(),
        is(expectedParameterDataTypes));

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(false);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerStatement(expectedMessageName, ((ParseMessage) message).getStatement());
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleParse();
  }

  @Test(expected = IllegalStateException.class)
  public void testParseMessageExceptsIfNameIsInUse() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload = "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length = intToBytes(
        4
            + statementName.length()
            + payload.length()
            + parameterCount.length
            + parameters.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementName.getBytes(),
        payload.getBytes(),
        parameterCount,
        parameters
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);
    message.send();
  }

  @Test(expected = IllegalStateException.class)
  public void testParseMessageExceptsIfNameIsNull() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload = "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length = intToBytes(
        4
            + statementName.length()
            + payload.length()
            + parameterCount.length
            + parameters.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementName.getBytes(),
        payload.getBytes(),
        parameterCount,
        parameters
    );

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);
    message.send();
  }

  @Test
  public void testParseMessageWorksIfNameIsEmpty() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "\0";
    String payload = "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length = intToBytes(
        4
            + statementName.length()
            + payload.length()
            + parameterCount.length
            + parameters.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementName.getBytes(),
        payload.getBytes(),
        parameterCount,
        parameters
    );

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);

    new DataInputStream(new ByteArrayInputStream(value));

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);
    message.send();
  }

  @Test
  public void testBindMessage() throws Exception {
    byte[] messageMetadata = {'B'};
    String portalName = "some portal\0";
    String statementName = "some statement\0";

    byte[] parameterCodesCount = {0, 0}; // Denotes no codes

    byte[] parameterCount = {0, 1};
    byte[] parameter = "someUser\0".getBytes();
    byte[] parameterLength = intToBytes(parameter.length);

    byte[] resultCodesCount = {0, 0};

    byte[] length = intToBytes(
        4
            + portalName.length()
            + statementName.length()
            + parameterCodesCount.length
            + parameterCount.length
            + parameterLength.length
            + parameter.length
            + resultCodesCount.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        portalName.getBytes(),
        statementName.getBytes(),
        parameterCodesCount,
        parameterCount,
        parameterLength,
        parameter,
        resultCodesCount
    );

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePreparedStatement);

    byte[][] expectedParameters = {parameter};
    List<Short> expectedFormatCodes = new ArrayList<>();
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert.assertThat(((BindMessage) message).getResultFormatCodes(), is(expectedFormatCodes));

    Mockito.when(intermediatePreparedStatement.bind(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()))
        .thenReturn(intermediatePortalStatement);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerPortal(expectedPortalName, intermediatePortalStatement);
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleBind();
  }

  @Test
  public void testBindMessageOneNonTextParam() throws Exception {
    byte[] messageMetadata = {'B'};
    String portalName = "some portal\0";
    String statementName = "some statement\0";

    byte[] parameterCodesCount = {0, 2};
    byte[] parameterCodes = {0, 0, 0, 1}; // First is text, second binary

    byte[] parameterCount = {0, 2};
    byte[] firstParameter = "someUser\0".getBytes();
    byte[] firstParameterLength = intToBytes(firstParameter.length);
    byte[] secondParameter = {0, 1, 0, 1};
    byte[] secondParameterLength = intToBytes(secondParameter.length);

    byte[] resultCodesCount = {0, 1};
    byte[] resultCodes = {0, 1}; // binary

    byte[] length = intToBytes(
        4
            + portalName.length()
            + statementName.length()
            + parameterCodesCount.length
            + parameterCodes.length
            + parameterCount.length
            + firstParameterLength.length
            + firstParameter.length
            + secondParameterLength.length
            + secondParameter.length
            + resultCodesCount.length
            + resultCodes.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        portalName.getBytes(),
        statementName.getBytes(),
        parameterCodesCount,
        parameterCodes,
        parameterCount,
        firstParameterLength,
        firstParameter,
        secondParameterLength,
        secondParameter,
        resultCodesCount,
        resultCodes
    );

    byte[][] expectedParameters = {firstParameter, secondParameter};
    List<Short> expectedFormatCodes = Arrays.asList((short) 0, (short) 1);
    List<Short> expectedResultFormatCodes = Arrays.asList((short) 1);
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert
        .assertThat(((BindMessage) message).getResultFormatCodes(), is(expectedResultFormatCodes));
  }

  @Test
  public void testBindMessageAllNonTextParam() throws Exception {
    byte[] messageMetadata = {'B'};
    String portalName = "some portal\0";
    String statementName = "some statement\0";

    byte[] parameterCodesCount = {0, 1};
    byte[] parameterCodes = {0, 1}; // binary

    byte[] parameterCount = {0, 2};
    byte[] firstParameter = "someUser\0".getBytes();
    byte[] firstParameterLength = intToBytes(firstParameter.length);
    byte[] secondParameter = {0, 1, 0, 1};
    byte[] secondParameterLength = intToBytes(secondParameter.length);

    byte[] resultCodesCount = {0, 1};
    byte[] resultCodes = {0, 1}; // binary

    byte[] length = intToBytes(
        4
            + portalName.length()
            + statementName.length()
            + parameterCodesCount.length
            + parameterCodes.length
            + parameterCount.length
            + firstParameterLength.length
            + firstParameter.length
            + secondParameterLength.length
            + secondParameter.length
            + resultCodesCount.length
            + resultCodes.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        portalName.getBytes(),
        statementName.getBytes(),
        parameterCodesCount,
        parameterCodes,
        parameterCount,
        firstParameterLength,
        firstParameter,
        secondParameterLength,
        secondParameter,
        resultCodesCount,
        resultCodes
    );

    byte[][] expectedParameters = {firstParameter, secondParameter};
    List<Short> expectedFormatCodes = Arrays.asList((short) 1);
    List<Short> expectedResultFormatCodes = Arrays.asList((short) 1);
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert
        .assertThat(((BindMessage) message).getResultFormatCodes(), is(expectedResultFormatCodes));
  }

  @Test
  public void testDescribePortalMessage() throws Exception {
    byte[] messageMetadata = {'D'};
    byte[] statementType = {'P'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(
        4
            + 1
            + statementName.length());

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementType,
        statementName.getBytes());

    String expectedStatementName = "some statement";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), DescribeMessage.class);
    Assert.assertEquals(((DescribeMessage) message).getName(), expectedStatementName);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some statement");

    Mockito.when(intermediatePortalStatement.describe()).thenReturn(describePortalMetadata);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleDescribePortal(intermediatePortalStatement, describePortalMetadata);
  }

  @Test
  public void testDescribeStatementMessage() throws Exception {
    byte[] messageMetadata = {'D'};
    byte[] statementType = {'S'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(
        4
            + 1
            + statementName.length()
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementType,
        statementName.getBytes()
    );

    String expectedStatementName = "some statement";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePreparedStatement);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), DescribeMessage.class);
    Assert.assertEquals(((DescribeMessage) message).getName(), expectedStatementName);

    Mockito.verify(connectionHandler, Mockito.times(1)).getStatement("some statement");

    Mockito.when(intermediatePreparedStatement.describe()).thenReturn(describeStatementMetadata);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleDescribeStatement(describeStatementMetadata);
  }

  @Test
  public void testExecuteMessage() throws Exception {
    byte[] messageMetadata = {'E'};
    String statementName = "some portal\0";
    int totalRows = 99999;

    byte[] length = intToBytes(
        4
            + statementName.length()
            + 4
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementName.getBytes(),
        intToBytes(totalRows)
    );

    String expectedStatementName = "some portal";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), ExecuteMessage.class);
    Assert.assertEquals(((ExecuteMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((ExecuteMessage) message).getMaxRows(), totalRows);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some portal");

    message.send();
    Mockito.verify(intermediatePortalStatement, Mockito.times(1))
        .execute();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleExecute(intermediatePortalStatement, totalRows);
  }

  @Test
  public void testClosePortalMessage() throws Exception {
    byte[] messageMetadata = {'C'};
    byte[] statementType = {'P'};
    String statementName = "some portal\0";

    byte[] length = intToBytes(
        4
            + statementType.length
            + statementName.length()
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementType,
        statementName.getBytes()
    );

    String expectedStatementName = "some portal";
    PreparedType expectedType = PreparedType.Portal;
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), CloseMessage.class);
    Assert.assertEquals(((CloseMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((CloseMessage) message).getType(), expectedType);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some portal");

    message.send();
    Mockito.verify(intermediatePortalStatement, Mockito.times(1))
        .close();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .closePortal(expectedStatementName);
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleClose();
  }

  @Test
  public void testCloseStatementMessage() throws Exception {
    byte[] messageMetadata = {'C'};
    byte[] statementType = {'S'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(
        4
            + statementType.length
            + statementName.length()
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        statementType,
        statementName.getBytes()
    );

    String expectedStatementName = "some statement";
    PreparedType expectedType = PreparedType.Statement;
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePortalStatement);

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), CloseMessage.class);
    Assert.assertEquals(((CloseMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((CloseMessage) message).getType(), expectedType);

    Mockito.verify(connectionHandler, Mockito.times(1)).getStatement("some statement");

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .closeStatement(expectedStatementName);
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleClose();
  }

  @Test
  public void testSyncMessage() throws Exception {
    byte[] messageMetadata = {'S'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(
        messageMetadata,
        length
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), SyncMessage.class);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleSync();
  }

  @Test
  public void testFlushMessage() throws Exception {
    byte[] messageMetadata = {'H'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(
        messageMetadata,
        length
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), FlushMessage.class);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleFlush();
  }

  @Test
  public void testTerminateMessage() throws Exception {
    byte[] messageMetadata = {'X'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(
        messageMetadata,
        length
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);
    Assert.assertEquals(message.getClass(), TerminateMessage.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownMessageTypeCausesException() throws Exception {
    byte[] messageMetadata = {'Y'};

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(messageMetadata));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .handleTerminate();
  }

  @Test(expected = IllegalStateException.class)
  public void testCopyDataMessagePopulatesButThrowsException() throws Exception {
    byte[] messageMetadata = {'d'};
    byte[] payload = "This is the payload".getBytes();

    byte[] length = intToBytes(
        4
            + payload.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        payload
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Assert.assertEquals(message.getClass(), CopyDataMessage.class);
    Assert.assertArrayEquals(((CopyDataMessage) message).getPayload(), payload);
    message.send();
  }

  @Test(expected = IllegalStateException.class)
  public void testCopyDoneMessageThrowsException() throws Exception {
    byte[] messageMetadata = {'c'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(
        messageMetadata,
        length
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Assert.assertEquals(message.getClass(), CopyDoneMessage.class);
    message.send();
  }

  @Test(expected = IllegalStateException.class)
  public void testCopyFailMessageThrowsException() throws Exception {
    byte[] messageMetadata = {'f'};
    byte[] errorMessage = "Error Message\0".getBytes();

    byte[] length = intToBytes(
        4
            + errorMessage.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        errorMessage
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    String expectedErrorMessage = "Error Message";

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Assert.assertEquals(message.getClass(), CopyFailMessage.class);
    Assert.assertEquals(((CopyFailMessage) message).getErrorMessage(), expectedErrorMessage);
    message.send();
  }

  @Test(expected = IllegalStateException.class)
  public void testFunctionCallMessageThrowsException() throws Exception {
    byte[] messageMetadata = {'F'};
    byte[] functionId = intToBytes(1);
    byte[] argumentCodesCount = {0, 2};
    byte[] argumentCodes = {0, 0, 0, 1}; // First is text, second binary
    byte[] parameterCount = {0, 2};
    byte[] firstParameter = "first parameter\0".getBytes();
    byte[] secondParameter = intToBytes(10);
    byte[] firstParameterLength = intToBytes(firstParameter.length);
    byte[] secondParameterLength = intToBytes(secondParameter.length);
    byte[] resultCode = {0, 0};

    byte[] length = intToBytes(
        4
            + functionId.length
            + argumentCodesCount.length
            + argumentCodes.length
            + parameterCount.length
            + firstParameterLength.length
            + firstParameter.length
            + secondParameterLength.length
            + secondParameter.length
            + resultCode.length
    );

    byte[] value = Bytes.concat(
        messageMetadata,
        length,
        functionId,
        argumentCodesCount,
        argumentCodes,
        parameterCount,
        firstParameterLength,
        firstParameter,
        secondParameterLength,
        secondParameter,
        resultCode
    );

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    WireMessage message = WireMessage.create(connectionHandler, inputStream);

    Assert.assertEquals(message.getClass(), FunctionCallMessage.class);
    message.send();
  }

}
