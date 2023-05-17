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

package com.google.cloud.spanner.pgadapter.statements;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.wireprotocol.BindMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.DescribeMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ExecuteMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.collect.ImmutableList;
import java.io.DataOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class ExtendedQueryProtocolHandlerTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private ConnectionHandler connectionHandler;
  @Mock private BackendConnection backendConnection;

  @Test
  public void testBuffer() {
    ParseMessage parseMessage = mock(ParseMessage.class);
    BindMessage bindMessage = mock(BindMessage.class);
    DescribeMessage describeMessage = mock(DescribeMessage.class);
    ExecuteMessage executeMessage = mock(ExecuteMessage.class);

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.buffer(parseMessage);
    handler.buffer(bindMessage);
    handler.buffer(describeMessage);
    handler.buffer(executeMessage);

    assertEquals(
        ImmutableList.of(parseMessage, bindMessage, describeMessage, executeMessage),
        handler.getMessages());
  }

  @Test
  public void testFlush() throws Exception {
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.getOutputStream()).thenReturn(mock(DataOutputStream.class));
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParseMessage parseMessage = mock(ParseMessage.class);
    BindMessage bindMessage = mock(BindMessage.class);
    DescribeMessage describeMessage = mock(DescribeMessage.class);
    ExecuteMessage executeMessage = mock(ExecuteMessage.class);

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.buffer(parseMessage);
    handler.buffer(bindMessage);
    handler.buffer(describeMessage);
    handler.buffer(executeMessage);
    assertEquals(
        ImmutableList.of(parseMessage, bindMessage, describeMessage, executeMessage),
        handler.getMessages());

    handler.flush();

    assertEquals(0, handler.getMessages().size());
    verify(backendConnection).flush();
    verify(backendConnection, never()).sync();
    verify(parseMessage).flush();
    verify(bindMessage).flush();
    verify(describeMessage).flush();
    verify(executeMessage).flush();
  }

  @Test
  public void testSync() throws Exception {
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.getOutputStream()).thenReturn(mock(DataOutputStream.class));
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParseMessage parseMessage = mock(ParseMessage.class);
    BindMessage bindMessage = mock(BindMessage.class);
    DescribeMessage describeMessage = mock(DescribeMessage.class);
    ExecuteMessage executeMessage = mock(ExecuteMessage.class);

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.buffer(parseMessage);
    handler.buffer(bindMessage);
    handler.buffer(describeMessage);
    handler.buffer(executeMessage);
    assertEquals(
        ImmutableList.of(parseMessage, bindMessage, describeMessage, executeMessage),
        handler.getMessages());

    handler.sync(false);

    assertEquals(0, handler.getMessages().size());
    verify(backendConnection, never()).flush();
    verify(backendConnection).sync();
    verify(parseMessage).flush();
    verify(bindMessage).flush();
    verify(describeMessage).flush();
    verify(executeMessage).flush();
  }

  @Test
  public void testInterrupted() {
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.getOutputStream()).thenReturn(mock(DataOutputStream.class));
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);
    ParseMessage parseMessage = mock(ParseMessage.class);

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.buffer(parseMessage);

    Thread.currentThread().interrupt();
    PGException exception = assertThrows(PGException.class, () -> handler.sync(false));
    assertEquals("Query cancelled", exception.getMessage());
    assertEquals(SQLState.QueryCanceled, exception.getSQLState());
  }
}
