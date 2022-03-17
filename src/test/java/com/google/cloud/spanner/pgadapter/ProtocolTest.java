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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ResultType;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.MatcherStatement;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CancelMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CloseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage.PreparedType;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyDataMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyDoneMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.CopyFailMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FlushMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.FunctionCallMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.TerminateMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.AfterClass;
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

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private ConnectionHandler connectionHandler;
  @Mock private Connection connection;
  @Mock private ProxyServer server;
  @Mock private OptionsMetadata options;
  @Mock private IntermediateStatement intermediateStatement;
  @Mock private IntermediatePreparedStatement intermediatePreparedStatement;
  @Mock private IntermediatePortalStatement intermediatePortalStatement;
  @Mock private DescribeStatementMetadata describeStatementMetadata;
  @Mock private DescribePortalMetadata describePortalMetadata;
  @Mock private ConnectionMetadata connectionMetadata;
  @Mock private DataOutputStream outputStream;
  @Mock private ResultSet resultSet;
  @Mock private StatementResult statementResult;
  @Mock private MutationWriter mutationWriter;

  private byte[] intToBytes(int value) {
    byte[] parameters = new byte[4];
    ByteConverter.int4(parameters, 0, value);
    return parameters;
  }

  private DataInputStream inputStreamFromOutputStream(ByteArrayOutputStream output) {
    return new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
  }

  private String readUntil(DataInputStream input, int length) throws Exception {
    byte[] item = new byte[length];
    input.read(item, 0, length);
    return new String(item, StandardCharsets.UTF_8);
  }

  private String readUntil(DataInputStream input, char delimeter) throws Exception {
    String result = "";
    byte c;
    do {
      c = input.readByte();
      result += (char) c;
    } while (c != '\0');
    return result;
  }

  @AfterClass
  public static void cleanup() {
    // TODO: Make error log file configurable and turn off writing to a file during tests.
    File outputFile = new File("output.txt");
    outputFile.delete();
  }

  @Test
  public void testQueryMessage() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, 24};
    String payload = "SELECT * FROM users\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    String expectedSQL = "SELECT * FROM users";

    Mockito.when(connection.execute(Statement.of(expectedSQL))).thenReturn(statementResult);
    when(statementResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    when(statementResult.getResultSet()).thenReturn(resultSet);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.requiresMatcher()).thenReturn(false);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), QueryMessage.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getSql(), expectedSQL);

    QueryMessage messageSpy = (QueryMessage) Mockito.spy(message);

    Mockito.doNothing().when(messageSpy).handleQuery();

    messageSpy.send();
    // Execute
    Mockito.verify(connection, Mockito.times(1)).execute(Statement.of(expectedSQL));
  }

  @Test
  public void testQueryUsesPSQLStatementWhenPSQLModeSelectedMessage() throws Exception {
    JSONParser parser = new JSONParser();
    byte[] messageMetadata = {'Q', 0, 0, 0, 24};
    String payload = "SELECT * FROM users\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    String expectedSQL = "SELECT * FROM users";

    when(connection.execute(Statement.of(expectedSQL))).thenReturn(statementResult);
    when(statementResult.getResultType()).thenReturn(ResultType.RESULT_SET);
    Mockito.when(statementResult.getResultSet()).thenReturn(resultSet);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.requiresMatcher()).thenReturn(true);
    Mockito.when(options.getCommandMetadataJSON())
        .thenReturn((JSONObject) parser.parse("{\"commands\": []}"));
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), QueryMessage.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getClass(), MatcherStatement.class);
    Assert.assertEquals(((QueryMessage) message).getStatement().getSql(), expectedSQL);

    QueryMessage messageSpy = (QueryMessage) Mockito.spy(message);

    Mockito.doNothing().when(messageSpy).handleQuery();

    messageSpy.send();
    Mockito.verify(connection, Mockito.times(1)).execute(Statement.of(expectedSQL));
  }

  @Test(expected = IOException.class)
  public void testQueryMessageFailsWhenNotNullTerminated() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, 23};
    String payload = "SELECT * FROM users";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.requiresMatcher()).thenReturn(false);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    ControlMessage.create(connectionHandler);
  }

  @Test
  public void testParseMessageException() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload = "SELECT * FROM users WHERE name = $1\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    List<Integer> expectedParameterDataTypes = Arrays.asList(1002);
    String expectedSQL = "SELECT * FROM users WHERE name = $1";
    String expectedMessageName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), ParseMessage.class);
    Assert.assertEquals(((ParseMessage) message).getName(), expectedMessageName);
    Assert.assertEquals(((ParseMessage) message).getStatement().getSql(), expectedSQL);
    Assert.assertThat(
        ((ParseMessage) message).getStatement().getParameterDataTypes(),
        is(expectedParameterDataTypes));

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(false);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerStatement(expectedMessageName, ((ParseMessage) message).getStatement());

    // ParseCompleteResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '1');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test
  public void testParseMessage() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    List<Integer> expectedParameterDataTypes = Arrays.asList(1002);
    String expectedSQL = "SELECT * FROM users WHERE name = $1";
    String expectedMessageName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), ParseMessage.class);
    Assert.assertEquals(((ParseMessage) message).getName(), expectedMessageName);
    Assert.assertEquals(((ParseMessage) message).getStatement().getSql(), expectedSQL);
    Assert.assertThat(
        ((ParseMessage) message).getStatement().getParameterDataTypes(),
        is(expectedParameterDataTypes));

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(false);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerStatement(expectedMessageName, ((ParseMessage) message).getStatement());

    // ParseCompleteResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '1');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test
  public void testParseMessageAcceptsUntypedParameter() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    // Unspecifed parameter type.
    byte[] parameters = intToBytes(0);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    List<Integer> expectedParameterDataTypes = Arrays.asList(0);
    String expectedSQL = "SELECT * FROM users WHERE name = $1";
    String expectedMessageName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), ParseMessage.class);
    Assert.assertEquals(((ParseMessage) message).getName(), expectedMessageName);
    Assert.assertEquals(((ParseMessage) message).getStatement().getSql(), expectedSQL);
    Assert.assertThat(
        ((ParseMessage) message).getStatement().getParameterDataTypes(),
        is(expectedParameterDataTypes));

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(false);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerStatement(expectedMessageName, ((ParseMessage) message).getStatement());

    // ParseCompleteResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '1');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test
  public void testParseMessageWithNonMatchingParameterTypeCount() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] length = intToBytes(4 + statementName.length() + payload.length() + 1);

    byte[] value =
        Bytes.concat(
            messageMetadata, length, statementName.getBytes(), payload.getBytes(), intToBytes(0));

    List<Integer> expectedParameterDataTypes = Collections.emptyList();
    String expectedSQL = "SELECT * FROM users WHERE name = $1";
    String expectedMessageName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), ParseMessage.class);
    Assert.assertEquals(((ParseMessage) message).getName(), expectedMessageName);
    Assert.assertEquals(((ParseMessage) message).getStatement().getSql(), expectedSQL);
    Assert.assertThat(
        ((ParseMessage) message).getStatement().getParameterDataTypes(),
        is(expectedParameterDataTypes));

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(false);
    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerStatement(expectedMessageName, ((ParseMessage) message).getStatement());

    // ParseCompleteResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '1');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test(expected = IllegalStateException.class)
  public void testParseMessageExceptsIfNameIsInUse() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);
    message.send();
  }

  @Test(expected = IllegalStateException.class)
  public void testParseMessageExceptsIfNameIsNull() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "some statement\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);
    message.send();
  }

  @Test
  public void testParseMessageWorksIfNameIsEmpty() throws Exception {
    byte[] messageMetadata = {'P'};
    String statementName = "\0";
    String payload =
        "SELECT * FROM users WHERE name = $1 /*This is a comment*/ --this is another comment\0";

    byte[] parameterCount = {0, 1};
    byte[] parameters = intToBytes(1002);

    byte[] length =
        intToBytes(
            4
                + statementName.length()
                + payload.length()
                + parameterCount.length
                + parameters.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            statementName.getBytes(),
            payload.getBytes(),
            parameterCount,
            parameters);

    Mockito.when(connectionHandler.hasStatement(anyString())).thenReturn(true);

    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

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

    byte[] length =
        intToBytes(
            4
                + portalName.length()
                + statementName.length()
                + parameterCodesCount.length
                + parameterCount.length
                + parameterLength.length
                + parameter.length
                + resultCodesCount.length);

    byte[] value =
        Bytes.concat(
            messageMetadata,
            length,
            portalName.getBytes(),
            statementName.getBytes(),
            parameterCodesCount,
            parameterCount,
            parameterLength,
            parameter,
            resultCodesCount);

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePreparedStatement);

    byte[][] expectedParameters = {parameter};
    List<Short> expectedFormatCodes = new ArrayList<>();
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert.assertThat(((BindMessage) message).getResultFormatCodes(), is(expectedFormatCodes));

    Mockito.when(
            intermediatePreparedStatement.bind(
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(intermediatePortalStatement);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1))
        .registerPortal(expectedPortalName, intermediatePortalStatement);

    // BindCompleteResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '2');
    Assert.assertEquals(outputResult.readInt(), 4);
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

    byte[] length =
        intToBytes(
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
                + resultCodes.length);

    byte[] value =
        Bytes.concat(
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
            resultCodes);

    byte[][] expectedParameters = {firstParameter, secondParameter};
    List<Short> expectedFormatCodes = Arrays.asList((short) 0, (short) 1);
    List<Short> expectedResultFormatCodes = Arrays.asList((short) 1);
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert.assertThat(
        ((BindMessage) message).getResultFormatCodes(), is(expectedResultFormatCodes));
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

    byte[] length =
        intToBytes(
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
                + resultCodes.length);

    byte[] value =
        Bytes.concat(
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
            resultCodes);

    byte[][] expectedParameters = {firstParameter, secondParameter};
    List<Short> expectedFormatCodes = Arrays.asList((short) 1);
    List<Short> expectedResultFormatCodes = Arrays.asList((short) 1);
    String expectedPortalName = "some portal";
    String expectedStatementName = "some statement";

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), BindMessage.class);
    Assert.assertEquals(((BindMessage) message).getPortalName(), expectedPortalName);
    Assert.assertEquals(((BindMessage) message).getStatementName(), expectedStatementName);
    Assert.assertThat(((BindMessage) message).getParameters(), is(expectedParameters));
    Assert.assertThat(((BindMessage) message).getFormatCodes(), is(expectedFormatCodes));
    Assert.assertThat(
        ((BindMessage) message).getResultFormatCodes(), is(expectedResultFormatCodes));
  }

  @Test
  public void testDescribePortalMessage() throws Exception {
    byte[] messageMetadata = {'D'};
    byte[] statementType = {'P'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(4 + 1 + statementName.length());

    byte[] value = Bytes.concat(messageMetadata, length, statementType, statementName.getBytes());

    String expectedStatementName = "some statement";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), DescribeMessage.class);
    Assert.assertEquals(((DescribeMessage) message).getName(), expectedStatementName);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some statement");

    Mockito.when(intermediatePortalStatement.describe()).thenReturn(describePortalMetadata);
    DescribeMessage messageSpy = (DescribeMessage) Mockito.spy(message);

    Mockito.doNothing().when(messageSpy).handleDescribePortal();

    messageSpy.send();

    Mockito.verify(messageSpy, Mockito.times(1)).handleDescribePortal();
  }

  @Test
  public void testDescribeStatementMessage() throws Exception {
    byte[] messageMetadata = {'D'};
    byte[] statementType = {'S'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(4 + 1 + statementName.length());

    byte[] value = Bytes.concat(messageMetadata, length, statementType, statementName.getBytes());

    String expectedStatementName = "some statement";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePreparedStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), DescribeMessage.class);
    Assert.assertEquals(((DescribeMessage) message).getName(), expectedStatementName);

    Mockito.verify(connectionHandler, Mockito.times(1)).getStatement("some statement");

    Mockito.when(intermediatePreparedStatement.describe()).thenReturn(describeStatementMetadata);
    DescribeMessage messageSpy = (DescribeMessage) Mockito.spy(message);

    Mockito.doNothing().when(messageSpy).handleDescribeStatement();

    messageSpy.send();

    Mockito.verify(messageSpy, Mockito.times(1)).handleDescribeStatement();
  }

  @Test
  public void testExecuteMessage() throws Exception {
    byte[] messageMetadata = {'E'};
    String statementName = "some portal\0";
    int totalRows = 99999;

    byte[] length = intToBytes(4 + statementName.length() + 4);

    byte[] value =
        Bytes.concat(messageMetadata, length, statementName.getBytes(), intToBytes(totalRows));

    String expectedStatementName = "some portal";
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), ExecuteMessage.class);
    Assert.assertEquals(((ExecuteMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((ExecuteMessage) message).getMaxRows(), totalRows);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some portal");
    ExecuteMessage messageSpy = (ExecuteMessage) Mockito.spy(message);

    Mockito.doReturn(false)
        .when(messageSpy)
        .sendSpannerResult(any(IntermediatePortalStatement.class), any(QueryMode.class), anyLong());

    messageSpy.send();

    Mockito.verify(intermediatePortalStatement, Mockito.times(1)).execute();
    Mockito.verify(messageSpy, Mockito.times(1))
        .sendSpannerResult(intermediatePortalStatement, QueryMode.EXTENDED, totalRows);
    Mockito.verify(connectionHandler, Mockito.times(1)).cleanUp(intermediatePortalStatement);
  }

  @Test
  public void testClosePortalMessage() throws Exception {
    byte[] messageMetadata = {'C'};
    byte[] statementType = {'P'};
    String statementName = "some portal\0";

    byte[] length = intToBytes(4 + statementType.length + statementName.length());

    byte[] value = Bytes.concat(messageMetadata, length, statementType, statementName.getBytes());

    String expectedStatementName = "some portal";
    PreparedType expectedType = PreparedType.Portal;
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getPortal(anyString())).thenReturn(intermediatePortalStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), CloseMessage.class);
    Assert.assertEquals(((CloseMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((CloseMessage) message).getType(), expectedType);

    Mockito.verify(connectionHandler, Mockito.times(1)).getPortal("some portal");

    message.send();
    Mockito.verify(intermediatePortalStatement, Mockito.times(1)).close();
    Mockito.verify(connectionHandler, Mockito.times(1)).closePortal(expectedStatementName);

    // CloseResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '3');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test
  public void testCloseStatementMessage() throws Exception {
    byte[] messageMetadata = {'C'};
    byte[] statementType = {'S'};
    String statementName = "some statement\0";

    byte[] length = intToBytes(4 + statementType.length + statementName.length());

    byte[] value = Bytes.concat(messageMetadata, length, statementType, statementName.getBytes());

    String expectedStatementName = "some statement";
    PreparedType expectedType = PreparedType.Statement;
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getStatement(anyString()))
        .thenReturn(intermediatePortalStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), CloseMessage.class);
    Assert.assertEquals(((CloseMessage) message).getName(), expectedStatementName);
    Assert.assertEquals(((CloseMessage) message).getType(), expectedType);

    Mockito.verify(connectionHandler, Mockito.times(1)).getStatement("some statement");

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1)).closeStatement(expectedStatementName);

    // CloseResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), '3');
    Assert.assertEquals(outputResult.readInt(), 4);
  }

  @Test
  public void testSyncMessage() throws Exception {
    byte[] messageMetadata = {'S'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), SyncMessage.class);

    message.send();

    // ReadyResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), 'Z');
    Assert.assertEquals(outputResult.readInt(), 5);
    Assert.assertEquals(outputResult.readByte(), 'I');
  }

  @Test
  public void testSyncMessageInTransaction() throws Exception {
    byte[] messageMetadata = {'S'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.isInTransaction()).thenReturn(true);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    assertEquals(SyncMessage.class, message.getClass());

    message.send();

    // ReadyResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    assertEquals('Z', outputResult.readByte());
    assertEquals(5, outputResult.readInt());
    assertEquals('T', outputResult.readByte());
  }

  @Test
  public void testFlushMessage() throws Exception {
    byte[] messageMetadata = {'H'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), FlushMessage.class);

    message.send();

    // ReadyResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    Assert.assertEquals(outputResult.readByte(), 'Z');
    Assert.assertEquals(outputResult.readInt(), 5);
    Assert.assertEquals(outputResult.readByte(), 'I');
  }

  @Test
  public void testFlushMessageInTransaction() throws Exception {
    byte[] messageMetadata = {'H'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.isInTransaction()).thenReturn(true);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    assertEquals(FlushMessage.class, message.getClass());

    message.send();

    // ReadyResponse
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    assertEquals('Z', outputResult.readByte());
    assertEquals(5, outputResult.readInt());
    assertEquals('T', outputResult.readByte());
  }

  @Test
  public void testQueryMessageInTransaction() throws Exception {
    byte[] messageMetadata = {'Q', 0, 0, 0, 45};
    String payload = "INSERT INTO users (name) VALUES ('test')\0";
    byte[] value = Bytes.concat(messageMetadata, payload.getBytes());

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    String expectedSQL = "INSERT INTO users (name) VALUES ('test')";

    when(connection.isInTransaction()).thenReturn(true);
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(statementResult.getResultType()).thenReturn(ResultType.UPDATE_COUNT);
    when(statementResult.getUpdateCount()).thenReturn(1L);
    when(connection.execute(Statement.of(expectedSQL))).thenReturn(statementResult);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    when(connectionHandler.getServer()).thenReturn(server);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(server.getOptions()).thenReturn(options);
    when(options.getTextFormat()).thenReturn(TextFormat.POSTGRESQL);
    when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    assertEquals(QueryMessage.class, message.getClass());
    assertEquals(expectedSQL, ((QueryMessage) message).getStatement().getSql());

    message.send();

    // NoData response (query does not return any results).
    DataInputStream outputResult = inputStreamFromOutputStream(result);
    assertEquals('C', outputResult.readByte()); // CommandComplete
    assertEquals('\0', outputResult.readByte());
    assertEquals('\0', outputResult.readByte());
    assertEquals('\0', outputResult.readByte());
    // 15 = 4 + "INSERT".length() + " 0 1".length() + 1 (header + command length + null terminator)
    assertEquals(15, outputResult.readByte());
    byte[] command = new byte[10];
    assertEquals(10, outputResult.read(command, 0, 10));
    assertEquals("INSERT 0 1", new String(command));
    assertEquals('\0', outputResult.readByte());
    // ReadyResponse in transaction ('T')
    assertEquals('Z', outputResult.readByte());
    assertEquals(5, outputResult.readInt());
    assertEquals('T', outputResult.readByte());
  }

  @Test
  public void testTerminateMessage() throws Exception {
    byte[] messageMetadata = {'X'};

    byte[] length = intToBytes(4);

    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), TerminateMessage.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownMessageTypeCausesException() throws Exception {
    byte[] messageMetadata = {'Y'};

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(messageMetadata));

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

    message.send();
    Mockito.verify(connectionHandler, Mockito.times(1)).handleTerminate();
  }

  @Test
  public void testCopyDataMessage() throws Exception {
    byte[] messageMetadata = {'d'};
    byte[] payload = "This is the payload".getBytes();
    byte[] length = intToBytes(4 + payload.length);
    byte[] value = Bytes.concat(messageMetadata, length, payload);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    CopyStatement copyStatement = Mockito.mock(CopyStatement.class);
    Mockito.when(connectionHandler.getActiveStatement()).thenReturn(copyStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getStatus()).thenReturn(ConnectionStatus.COPY_IN);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    MutationWriter mw = Mockito.mock(MutationWriter.class);
    Mockito.when(copyStatement.getMutationWriter()).thenReturn(mw);

    WireMessage message = ControlMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), CopyDataMessage.class);
    Assert.assertArrayEquals(((CopyDataMessage) message).getPayload(), payload);

    CopyDataMessage messageSpy = (CopyDataMessage) Mockito.spy(message);
    messageSpy.send();

    Mockito.verify(mw, Mockito.times(1)).addCopyData(payload);
  }

  @Test
  public void testMultipleCopyDataMessages() throws Exception {
    setupQueryInformationSchemaResults();
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    when(statementResult.getUpdateCount()).thenReturn(1L);
    when(connectionHandler.getStatus()).thenReturn(ConnectionStatus.COPY_IN);

    byte[] messageMetadata = {'d'};
    byte[] payload1 = "1\t'one'\n2\t".getBytes();
    byte[] payload2 = "'two'\n3\t'th".getBytes();
    byte[] payload3 = "ree'\n4\t'four'\n".getBytes();
    byte[] length1 = intToBytes(4 + payload1.length);
    byte[] length2 = intToBytes(4 + payload2.length);
    byte[] length3 = intToBytes(4 + payload3.length);
    byte[] value1 = Bytes.concat(messageMetadata, length1, payload1);
    byte[] value2 = Bytes.concat(messageMetadata, length2, payload2);
    byte[] value3 = Bytes.concat(messageMetadata, length3, payload3);

    DataInputStream inputStream1 = new DataInputStream(new ByteArrayInputStream(value1));
    DataInputStream inputStream2 = new DataInputStream(new ByteArrayInputStream(value2));
    DataInputStream inputStream3 = new DataInputStream(new ByteArrayInputStream(value3));

    ResultSet spannerType = Mockito.mock(ResultSet.class);
    Mockito.when(spannerType.getString("column_name")).thenReturn("key", "value");
    Mockito.when(spannerType.getString("data_type")).thenReturn("bigint", "character varying");
    Mockito.when(spannerType.next()).thenReturn(true, true, false);
    Mockito.when(connection.executeQuery(any(Statement.class))).thenReturn(spannerType);

    CopyStatement copyStatement =
        new CopyStatement(options, "COPY keyvalue FROM STDIN;", connection);
    copyStatement.execute();

    Mockito.when(connectionHandler.getActiveStatement()).thenReturn(copyStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    {
      Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream1);
      WireMessage message = ControlMessage.create(connectionHandler);
      Assert.assertEquals(message.getClass(), CopyDataMessage.class);
      Assert.assertArrayEquals(((CopyDataMessage) message).getPayload(), payload1);
      CopyDataMessage copyDataMessage = (CopyDataMessage) message;
      copyDataMessage.send();
    }
    {
      Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream2);
      WireMessage message = ControlMessage.create(connectionHandler);
      Assert.assertEquals(message.getClass(), CopyDataMessage.class);
      Assert.assertArrayEquals(((CopyDataMessage) message).getPayload(), payload2);
      CopyDataMessage copyDataMessage = (CopyDataMessage) message;
      copyDataMessage.send();
    }
    {
      Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream3);
      WireMessage message = ControlMessage.create(connectionHandler);
      Assert.assertEquals(message.getClass(), CopyDataMessage.class);
      Assert.assertArrayEquals(((CopyDataMessage) message).getPayload(), payload3);
      CopyDataMessage copyDataMessage = (CopyDataMessage) message;
      copyDataMessage.send();
    }
  }

  @Test
  public void testCopyDoneMessage() throws Exception {
    byte[] messageMetadata = {'c'};
    byte[] length = intToBytes(4);
    byte[] value = Bytes.concat(messageMetadata, length);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    CopyStatement copyStatement = Mockito.mock(CopyStatement.class);
    Mockito.when(connectionHandler.getActiveStatement()).thenReturn(copyStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getStatus()).thenReturn(ConnectionStatus.COPY_IN);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    MutationWriter mb = Mockito.mock(MutationWriter.class);
    Mockito.when(copyStatement.getMutationWriter()).thenReturn(mb);
    WireMessage message = ControlMessage.create(connectionHandler);

    Assert.assertEquals(message.getClass(), CopyDoneMessage.class);
    CopyDoneMessage messageSpy = (CopyDoneMessage) Mockito.spy(message);
    Mockito.doReturn(false)
        .when(messageSpy)
        .sendSpannerResult(any(IntermediateStatement.class), any(QueryMode.class), anyLong());

    messageSpy.send();
    Mockito.verify(messageSpy, Mockito.times(1))
        .sendSpannerResult(copyStatement, QueryMode.SIMPLE, 0L);
  }

  @Test
  public void testCopyFailMessage() throws Exception {
    byte[] messageMetadata = {'f'};
    byte[] errorMessage = "Error Message\0".getBytes();
    byte[] length = intToBytes(4 + errorMessage.length);
    byte[] value = Bytes.concat(messageMetadata, length, errorMessage);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    String expectedErrorMessage = "Error Message";

    CopyStatement copyStatement = Mockito.mock(CopyStatement.class);
    MutationWriter mutationWriter = Mockito.mock(MutationWriter.class);
    Mockito.when(copyStatement.getMutationWriter()).thenReturn(mutationWriter);
    Mockito.when(connectionHandler.getActiveStatement()).thenReturn(copyStatement);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getStatus()).thenReturn(ConnectionStatus.COPY_IN);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

    Assert.assertEquals(message.getClass(), CopyFailMessage.class);
    Assert.assertEquals(((CopyFailMessage) message).getErrorMessage(), expectedErrorMessage);
    message.send();

    verify(mutationWriter).rollback();
  }

  @Test
  public void testCopyFromFilePipe() throws Exception {
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(statementResult.getUpdateCount()).thenReturn(1L);
    setupQueryInformationSchemaResults();

    byte[] payload = Files.readAllBytes(Paths.get("./src/test/resources/small-file-test.txt"));

    CopyStatement copyStatement =
        new CopyStatement(mock(OptionsMetadata.class), "COPY keyvalue FROM STDIN;", connection);
    copyStatement.execute();

    MutationWriter mw = copyStatement.getMutationWriter();
    mw.addCopyData(payload);

    Assert.assertEquals(copyStatement.getFormatType(), "TEXT");
    Assert.assertEquals(copyStatement.getDelimiterChar(), '\t');

    copyStatement.close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testCopyBatchSizeLimit() throws Exception {
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    when(statementResult.getUpdateCount()).thenReturn(1L);
    setupQueryInformationSchemaResults();

    byte[] payload = Files.readAllBytes(Paths.get("./src/test/resources/batch-size-test.txt"));

    CopyStatement copyStatement =
        new CopyStatement(mock(OptionsMetadata.class), "COPY keyvalue FROM STDIN;", connection);

    Assert.assertFalse(copyStatement.isExecuted());
    copyStatement.execute();
    assertTrue(copyStatement.isExecuted());

    MutationWriter mw = copyStatement.getMutationWriter();
    // Inject a mock DatabaseClient for now.
    // TODO: Fix this once we can use the Connection API.
    Field databaseClientField = MutationWriter.class.getDeclaredField("databaseClient");
    databaseClientField.setAccessible(true);
    databaseClientField.set(mw, mock(DatabaseClient.class));
    mw.addCopyData(payload);
    mw.close();

    Assert.assertEquals(copyStatement.getFormatType(), "TEXT");
    Assert.assertEquals(copyStatement.getDelimiterChar(), '\t');
    Assert.assertFalse(copyStatement.hasException());
    Assert.assertEquals(12L, copyStatement.getUpdateCount().longValue());
    Assert.assertEquals(12L, mw.getRowCount());

    copyStatement.close();
  }

  @Test
  public void testCopyDataRowLengthMismatchLimit() throws Exception {
    setupQueryInformationSchemaResults();
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    Mockito.when(statementResult.getUpdateCount()).thenReturn(1L);

    byte[] payload = "1\t'one'\n2".getBytes();

    CopyStatement copyStatement =
        new CopyStatement(mock(OptionsMetadata.class), "COPY keyvalue FROM STDIN;", connection);

    Assert.assertFalse(copyStatement.isExecuted());
    copyStatement.execute();
    assertTrue(copyStatement.isExecuted());

    MutationWriter mw = copyStatement.getMutationWriter();
    mw.addCopyData(payload);
    mw.close();

    SpannerException thrown =
        Assert.assertThrows(SpannerException.class, copyStatement::getUpdateCount);
    Assert.assertEquals(ErrorCode.INVALID_ARGUMENT, thrown.getErrorCode());
    Assert.assertEquals(
        "INVALID_ARGUMENT: Invalid COPY data: Row length mismatched. Expected 2 columns, but only found 1",
        thrown.getMessage());

    copyStatement.close();

    File outputFile = new File("output.txt");
    outputFile.delete();
  }

  @Test
  public void testCopyResumeErrorOutputFile() throws Exception {
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    Mockito.when(statementResult.getUpdateCount()).thenReturn(1L);
    setupQueryInformationSchemaResults();

    byte[] payload = Files.readAllBytes(Paths.get("./src/test/resources/test-copy-output.txt"));

    CopyStatement copyStatement =
        new CopyStatement(mock(OptionsMetadata.class), "COPY keyvalue FROM STDIN;", connection);
    Assert.assertFalse(copyStatement.isExecuted());
    copyStatement.execute();
    assertTrue(copyStatement.isExecuted());

    File outputFile = new File("output.txt");
    outputFile.delete();

    MutationWriter mw = copyStatement.getMutationWriter();
    mw.addCopyData(payload);

    SpannerException thrown =
        Assert.assertThrows(SpannerException.class, copyStatement::getUpdateCount);
    Assert.assertEquals(ErrorCode.INVALID_ARGUMENT, thrown.getErrorCode());
    Assert.assertEquals(
        "INVALID_ARGUMENT: Invalid input syntax for type INT64:\"'5'\"", thrown.getMessage());

    outputFile = new File("output.txt");
    assertTrue(outputFile.exists());
    assertTrue(outputFile.isFile());

    assertTrue(outputFile.delete());
    copyStatement.close();
  }

  @Test
  public void testCopyResumeErrorStartOutputFile() throws Exception {
    setupQueryInformationSchemaResults();
    when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    when(connection.execute(any(Statement.class))).thenReturn(statementResult);
    when(statementResult.getUpdateCount()).thenReturn(1L);

    byte[] payload =
        Files.readAllBytes(Paths.get("./src/test/resources/test-copy-start-output.txt"));

    // Pre-emptively try to delete the output file if it is lingering from a previous test run.
    // TODO: Make the output file for COPY configurable, so we can use a temp file for this.
    File outputFile = new File("output.txt");
    outputFile.delete();

    CopyStatement copyStatement =
        new CopyStatement(options, "COPY keyvalue FROM STDIN;", connection);
    Assert.assertFalse(copyStatement.isExecuted());
    copyStatement.execute();
    assertTrue(copyStatement.isExecuted());

    MutationWriter mw = copyStatement.getMutationWriter();
    mw.addCopyData(payload);

    SpannerException thrown =
        Assert.assertThrows(SpannerException.class, copyStatement::getUpdateCount);
    Assert.assertEquals(ErrorCode.INVALID_ARGUMENT, thrown.getErrorCode());
    Assert.assertEquals(
        "INVALID_ARGUMENT: Invalid input syntax for type INT64:\"'1'\"", thrown.getMessage());

    outputFile = new File("output.txt");
    assertTrue(outputFile.exists());
    assertTrue(outputFile.isFile());

    assertTrue(outputFile.delete());
    copyStatement.close();
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

    byte[] length =
        intToBytes(
            4
                + functionId.length
                + argumentCodesCount.length
                + argumentCodes.length
                + parameterCount.length
                + firstParameterLength.length
                + firstParameter.length
                + secondParameterLength.length
                + secondParameter.length
                + resultCode.length);

    byte[] value =
        Bytes.concat(
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
            resultCode);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = ControlMessage.create(connectionHandler);

    Assert.assertEquals(message.getClass(), FunctionCallMessage.class);
    message.send();
  }

  @Test
  public void testStartUpMessage() throws Exception {
    byte[] protocol = intToBytes(196608);
    byte[] payload =
        ("database\0"
                + "databasename\0"
                + "application_name\0"
                + "psql\0"
                + "client_encoding\0"
                + "UTF8\0"
                + "server_version\0"
                + "13.4\0"
                + "user\0"
                + "me\0")
            .getBytes();
    byte[] length = intToBytes(8 + payload.length);

    byte[] value = Bytes.concat(length, protocol, payload);

    Map<String, String> expectedParameters = new HashMap<>();
    expectedParameters.put("database", "databasename");
    expectedParameters.put("application_name", "psql");
    expectedParameters.put("client_encoding", "UTF8");
    expectedParameters.put("server_version", "13.4");
    expectedParameters.put("user", "me");

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(connectionHandler.getConnectionId()).thenReturn(1);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.getServerVersion()).thenReturn("13.4");
    Mockito.when(options.shouldAuthenticate()).thenReturn(false);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = BootstrapMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), StartupMessage.class);

    Assert.assertEquals(((StartupMessage) message).getParameters(), expectedParameters);

    message.send();

    DataInputStream outputResult = inputStreamFromOutputStream(result);

    // AuthenticationOkResponse
    Assert.assertEquals(outputResult.readByte(), 'R');
    Assert.assertEquals(outputResult.readInt(), 8);
    Assert.assertEquals(outputResult.readInt(), 0);

    // KeyDataResponse
    Assert.assertEquals(outputResult.readByte(), 'K');
    Assert.assertEquals(outputResult.readInt(), 12);
    Assert.assertEquals(outputResult.readInt(), 1);
    Assert.assertEquals(outputResult.readInt(), 0);

    // ParameterStatusResponse (x11)
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 24);
    Assert.assertEquals(readUntil(outputResult, "server_version\0".length()), "server_version\0");
    Assert.assertEquals(readUntil(outputResult, "13.4\0".length()), "13.4\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 31);
    Assert.assertEquals(
        readUntil(outputResult, "application_name\0".length()), "application_name\0");
    Assert.assertEquals(readUntil(outputResult, "PGAdapter\0".length()), "PGAdapter\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 23);
    Assert.assertEquals(readUntil(outputResult, "is_superuser\0".length()), "is_superuser\0");
    Assert.assertEquals(readUntil(outputResult, "false\0".length()), "false\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 36);
    Assert.assertEquals(
        readUntil(outputResult, "session_authorization\0".length()), "session_authorization\0");
    Assert.assertEquals(readUntil(outputResult, "PGAdapter\0".length()), "PGAdapter\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 25);
    Assert.assertEquals(
        readUntil(outputResult, "integer_datetimes\0".length()), "integer_datetimes\0");
    Assert.assertEquals(readUntil(outputResult, "on\0".length()), "on\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 25);
    Assert.assertEquals(readUntil(outputResult, "server_encoding\0".length()), "server_encoding\0");
    Assert.assertEquals(readUntil(outputResult, "UTF8\0".length()), "UTF8\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 25);
    Assert.assertEquals(readUntil(outputResult, "client_encoding\0".length()), "client_encoding\0");
    Assert.assertEquals(readUntil(outputResult, "UTF8\0".length()), "UTF8\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 22);
    Assert.assertEquals(readUntil(outputResult, "DateStyle\0".length()), "DateStyle\0");
    Assert.assertEquals(readUntil(outputResult, "ISO,YMD\0".length()), "ISO,YMD\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 27);
    Assert.assertEquals(readUntil(outputResult, "IntervalStyle\0".length()), "IntervalStyle\0");
    Assert.assertEquals(readUntil(outputResult, "iso_8601\0".length()), "iso_8601\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 35);
    Assert.assertEquals(
        readUntil(outputResult, "standard_conforming_strings\0".length()),
        "standard_conforming_strings\0");
    Assert.assertEquals(readUntil(outputResult, "on\0".length()), "on\0");
    Assert.assertEquals(outputResult.readByte(), 'S');
    Assert.assertEquals(outputResult.readInt(), 17);
    Assert.assertEquals(readUntil(outputResult, "TimeZone\0".length()), "TimeZone\0");
    // Timezone will vary depending on the default location of the JVM that is running.
    readUntil(outputResult, '\0');

    // ReadyResponse
    Assert.assertEquals(outputResult.readByte(), 'Z');
    Assert.assertEquals(outputResult.readInt(), 5);
    Assert.assertEquals(outputResult.readByte(), 'I');
  }

  @Test
  public void testCancelMessage() throws Exception {
    byte[] length = intToBytes(16);
    byte[] protocol = intToBytes(80877102);
    byte[] connectionId = intToBytes(1);
    byte[] secret = intToBytes(1);

    byte[] value = Bytes.concat(length, protocol, connectionId, secret);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionHandler.getSecret()).thenReturn(1);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = BootstrapMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), CancelMessage.class);

    Assert.assertEquals(((CancelMessage) message).getConnectionId(), 1);
    Assert.assertEquals(((CancelMessage) message).getSecret(), 1);

    message.send();

    Mockito.verify(connectionHandler, Mockito.times(1)).cancelActiveStatement(1, 1);
    Mockito.verify(connectionHandler, Mockito.times(1)).handleTerminate();
  }

  @Test
  public void testSSLMessage() throws Exception {
    byte[] length = intToBytes(8);
    byte[] protocol = intToBytes(80877103);

    byte[] value = Bytes.concat(length, protocol);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = BootstrapMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), SSLMessage.class);

    message.send();

    DataInputStream outputResult = inputStreamFromOutputStream(result);

    // DeclineSSLResponse
    Assert.assertEquals(outputResult.readByte(), 'N');
  }

  @Test(expected = IOException.class)
  public void testSSLMessageFailsWhenCalledTwice() throws Exception {
    byte[] length = intToBytes(8);
    byte[] protocol = intToBytes(80877103);

    byte[] value = Bytes.concat(length, protocol);

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(value));
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(result);

    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);

    WireMessage message = BootstrapMessage.create(connectionHandler);
    Assert.assertEquals(message.getClass(), SSLMessage.class);

    message.send();

    DataInputStream outputResult = inputStreamFromOutputStream(result);

    // DeclineSSLResponse
    Assert.assertEquals(outputResult.readByte(), 'N');

    message.send();
  }

  private void setupQueryInformationSchemaResults() {
    ResultSet spannerType = Mockito.mock(ResultSet.class);
    Mockito.when(spannerType.getString("column_name")).thenReturn("key", "value");
    Mockito.when(spannerType.getString("data_type")).thenReturn("bigint", "character varying");
    Mockito.when(spannerType.next()).thenReturn(true, true, false);
    Mockito.when(
            connection.executeQuery(
                ArgumentMatchers.argThat(
                    statement ->
                        statement != null && statement.getSql().startsWith("SELECT column_name"))))
        .thenReturn(spannerType);

    ResultSet countResult = Mockito.mock(ResultSet.class);
    when(countResult.getLong(0)).thenReturn(2L);
    when(countResult.next()).thenReturn(true, false);
    Mockito.when(
            connection.executeQuery(
                ArgumentMatchers.argThat(
                    statement ->
                        statement != null && statement.getSql().startsWith("SELECT COUNT(*)"))))
        .thenReturn(countResult);
  }
}
