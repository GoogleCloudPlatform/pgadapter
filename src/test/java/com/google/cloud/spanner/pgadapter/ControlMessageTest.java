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

import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.ResultType;
import com.google.cloud.spanner.pgadapter.wireprotocol.ControlMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class ControlMessageTest {

  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private static final String EMPTY_COMMAND_JSON = "{\"commands\":[]}";
  private static final char QUERY_IDENTIFIER = 'Q';

  private ControlMessage controlMessage;
  private DataOutputStream outputStream;
  private DataInputStream inputStream;

  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Mock private ConnectionHandler connectionHandler;
  @Mock private IntermediateStatement intermediateStatement;
  @Mock private ConnectionMetadata connectionMetadata;
  @Mock private Connection connection;

  @Test
  public void testInsertResult() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(buffer);
    inputStream =
        new DataInputStream(
            new ByteArrayInputStream(new byte[] {(byte) QUERY_IDENTIFIER, 0, 0, 0, 5, 0}));

    Mockito.when(connectionMetadata.getInputStream()).thenReturn(inputStream);
    Mockito.when(connectionMetadata.getOutputStream()).thenReturn(outputStream);
    Mockito.when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    Mockito.when(intermediateStatement.getResultType()).thenReturn(ResultType.UPDATE_COUNT);
    Mockito.when(intermediateStatement.getCommand()).thenReturn("INSERT");
    Mockito.when(intermediateStatement.getUpdateCount()).thenReturn(1L);
    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);

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
    ProxyServer server = new ProxyServer(options);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);

    controlMessage = ControlMessage.create(connectionHandler);
    controlMessage.sendSpannerResult(intermediateStatement, QueryMode.SIMPLE, 0L);

    DataInputStream outputReader =
        new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));
    // identifier
    outputReader.readByte();
    // length
    outputReader.readInt();
    final String resultMessage = "INSERT 0 1";
    int numOfBytes = resultMessage.getBytes(UTF8).length;
    byte[] bytes = new byte[numOfBytes];
    outputReader.read(bytes, 0, numOfBytes);
    Assert.assertEquals(new String(bytes, UTF8), resultMessage);
  }
}
