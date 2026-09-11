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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import java.io.DataOutputStream;
import java.util.UUID;
import org.junit.Before;
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

  @Before
  public void setupMocks() {
    when(connectionHandler.getTraceConnectionId()).thenReturn(UUID.randomUUID());
  }

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

  @Test
  public void testBufferDoesNotCreateEventsForNonRecordingSpan() {
    Span span = startSpan(/* isRecording= */ false);
    ParseMessage parseMessage = mock(ParseMessage.class);

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.maybeStartSpan(true);
    handler.buffer(parseMessage);

    verify(span, never()).addEvent(anyString(), any(Attributes.class));
    // The attributes of the event contain the SQL statement. Getting the description and the SQL
    // statement must be skipped as well, as the event is dropped by the span anyway.
    verify(parseMessage, never()).receivedEventDescription();
    verify(parseMessage, never()).getSql();
  }

  @Test
  public void testBufferCreatesEventsForRecordingSpan() {
    Span span = startSpan(/* isRecording= */ true);
    ParseMessage parseMessage = mock(ParseMessage.class);
    when(parseMessage.receivedEventDescription()).thenReturn("Received message: 'P'");
    when(parseMessage.getSql()).thenReturn("select 1");

    ExtendedQueryProtocolHandler handler =
        new ExtendedQueryProtocolHandler(connectionHandler, backendConnection);
    handler.maybeStartSpan(true);
    handler.buffer(parseMessage);

    verify(span)
        .addEvent(
            "Received message: 'P'", Attributes.of(BackendConnection.DB_STATEMENT, "select 1"));
  }

  /** Sets up the mocks that are needed for {@link ExtendedQueryProtocolHandler#maybeStartSpan}. */
  private Span startSpan(boolean isRecording) {
    Span span = mock(Span.class);
    when(span.isRecording()).thenReturn(isRecording);
    SpanBuilder spanBuilder = mock(SpanBuilder.class);
    when(spanBuilder.setNoParent()).thenReturn(spanBuilder);
    when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);
    Tracer tracer = mock(Tracer.class);
    when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
    when(backendConnection.getTracer()).thenReturn(tracer);
    return span;
  }
}
