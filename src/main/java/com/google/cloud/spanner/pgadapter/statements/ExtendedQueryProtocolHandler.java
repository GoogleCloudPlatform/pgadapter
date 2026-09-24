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

import static com.google.cloud.spanner.pgadapter.Server.getVersion;
import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.DB_STATEMENT;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.utils.Logging;
import com.google.cloud.spanner.pgadapter.utils.Logging.Action;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireprotocol.AbstractQueryProtocolMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SyncMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles the message flow for the extended query protocol. Wire-protocol messages are buffered in
 * memory until a flush/sync is received.
 */
public class ExtendedQueryProtocolHandler {
  private static final Logger logger =
      Logger.getLogger(ExtendedQueryProtocolHandler.class.getName());

  @VisibleForTesting static final int DEFAULT_BUFFER_CAPACITY = 32;
  @VisibleForTesting static final int MAX_BUFFER_CAPACITY = 256;

  private final ArrayList<AbstractQueryProtocolMessage> messages;
  private final ConnectionHandler connectionHandler;
  private final BackendConnection backendConnection;

  private final String connectionId;
  private volatile Span span;
  private volatile Scope scope;
  private volatile Stopwatch stopwatch;

  /** Creates an {@link ExtendedQueryProtocolHandler} for the given connection. */
  public ExtendedQueryProtocolHandler(ConnectionHandler connectionHandler) {
    this(
        connectionHandler,
        new BackendConnection(
            connectionHandler
                .getServer()
                .getTracer(ConnectionHandler.class.getName(), getVersion()),
            connectionHandler.getServer().getMetrics(),
            createMetricAttributes(connectionHandler.getDatabaseId()),
            connectionHandler.getTraceConnectionId().toString(),
            connectionHandler::closeAllPortals,
            connectionHandler.getDatabaseId(),
            connectionHandler.getSpannerConnection(),
            connectionHandler::getWellKnownClient,
            connectionHandler.getServer().getOptions(),
            () -> connectionHandler.getWellKnownClient().getLocalStatements(connectionHandler)));
  }

  /** Constructor only intended for testing. */
  @VisibleForTesting
  public ExtendedQueryProtocolHandler(
      ConnectionHandler connectionHandler, BackendConnection backendConnection) {
    this(connectionHandler, backendConnection, new ArrayList<>(DEFAULT_BUFFER_CAPACITY));
  }

  @VisibleForTesting
  ExtendedQueryProtocolHandler(
      ConnectionHandler connectionHandler,
      BackendConnection backendConnection,
      ArrayList<AbstractQueryProtocolMessage> messages) {
    this.connectionHandler = Preconditions.checkNotNull(connectionHandler);
    this.connectionId = connectionHandler.getTraceConnectionId().toString();
    this.backendConnection = Preconditions.checkNotNull(backendConnection);
    this.messages = Preconditions.checkNotNull(messages);
  }

  /** Returns the backend PG connection for this query handler. */
  public BackendConnection getBackendConnection() {
    return backendConnection;
  }

  public Tracer getTracer() {
    return backendConnection.getTracer();
  }

  @VisibleForTesting
  static Attributes createMetricAttributes(DatabaseId databaseId) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    attributesBuilder.put("database", databaseId.getDatabase());
    attributesBuilder.put("instance_id", databaseId.getInstanceId().getInstance());
    attributesBuilder.put("project_id", databaseId.getInstanceId().getProject());
    return attributesBuilder.build();
  }

  /** Returns a copy of the currently buffered messages in this handler. */
  @VisibleForTesting
  List<AbstractQueryProtocolMessage> getMessages() {
    return new ArrayList<>(messages);
  }

  /**
   * Returns true if the last message in the buffer uses the extended query protocol. That is; it is
   * not a message that has been manually created by a {@link SimpleQueryStatement}.
   */
  boolean isExtendedProtocol() {
    return !this.messages.isEmpty()
        && this.messages.get(this.messages.size() - 1).isExtendedProtocol();
  }

  public void maybeStartSpan(boolean isExtendedProtocol) {
    if (span == null) {
      stopwatch = Stopwatch.createStarted();
      span =
          getBackendConnection()
              .getTracer()
              .spanBuilder("query_protocol_handler")
              .setNoParent()
              .setAttribute("pgadapter.query_protocol", isExtendedProtocol ? "extended" : "simple")
              .setAttribute("pgadapter.connection_id", connectionId)
              .startSpan();
      scope = span.makeCurrent();
    }
  }

  /**
   * Buffer an extended query protocol message for execution when the next flush/sync message is
   * received.
   */
  public void buffer(AbstractQueryProtocolMessage message) {
    addEvent(message.receivedEventDescription(), Attributes.of(DB_STATEMENT, message.getSql()));
    messages.add(message);
  }

  /**
   * Flushes the current queue of messages. Any pending database statements are first executed,
   * before sending the wire-protocol responses to the frontend. A flush does not commit the
   * implicit transaction (if any).
   *
   * <p>This method will execute a {@link #sync(boolean)} if it determines that the next message in
   * the buffer is a Sync message.
   */
  public void flush() throws Exception {
    addEvent("Received Flush");
    logger.log(Level.FINER, Logging.format("Flush", Action.Starting));
    if (isExtendedProtocol()) {
      // Wait at most 2 milliseconds for the next message to arrive. The method will just return 0
      // if no message could be found in the buffer within this timeframe.
      char nextMessage = connectionHandler.getConnectionMetadata().peekNextByte(2L);
      if (nextMessage == SyncMessage.IDENTIFIER) {
        // Do a sync instead of a flush, as the next message is a sync. This tells the backend
        // connection that it is safe to for example use a read-only transaction if the buffer only
        // contains queries.
        sync(false);
      } else {
        internalFlush();
      }
    } else {
      internalFlush();
    }
    logger.log(Level.FINER, Logging.format("Flush", Action.Finished));
  }

  private void internalFlush() throws Exception {
    backendConnection.flush();
    flushMessages();
  }

  /**
   * Flushes the current queue of messages and commits the implicit transaction (if any). Any
   * pending database statements are first executed, before sending the wire-protocol responses to
   * the frontend.
   */
  public void sync(boolean includeReadyResponse) throws Exception {
    addEvent("Received Sync");
    logger.log(Level.FINER, Logging.format("Sync", Action.Starting));
    backendConnection.sync();
    flushMessages(includeReadyResponse);
    logger.log(Level.FINER, Logging.format("Sync", Action.Finished));
  }

  /** Flushes the wire-protocol messages to the frontend. */
  private void flushMessages() throws Exception {
    flushMessages(false);
  }

  private void flushMessages(boolean includeReadyResponse) throws Exception {
    addEvent("Flushing messages");
    logger.log(Level.FINER, Logging.format("Flushing messages", Action.Starting));
    try {
      for (int i = 0; i < messages.size(); i++) {
        AbstractQueryProtocolMessage message = messages.get(i);
        logger.log(
            Level.FINEST,
            Logging.format(
                "Flushing message", Action.Starting, () -> String.format("Message: %s", message)));
        message.flush();
        logger.log(
            Level.FINEST,
            Logging.format(
                "Flushing message", Action.Finished, () -> String.format("Message: %s", message)));
        if (message.isReturnedErrorResponse()) {
          // Abort remaining messages in reverse (LIFO) order to correctly unwind state mutations in
          // the opposite order that they were registered/buffered (e.g. if a pipeline contains a
          // Close followed by a Parse/Bind reusing the same statement or portal name, or both
          // creation and closure of a statement in the same aborted pipeline).
          for (int j = messages.size() - 1; j > i; j--) {
            AbstractQueryProtocolMessage messageToAbort = messages.get(j);
            try {
              messageToAbort.abort();
            } catch (Exception exception) {
              logger.log(
                  Level.WARNING,
                  exception,
                  () -> String.format("Failed to abort message: %s", messageToAbort));
            }
          }
          break;
        }
      }
      if (Thread.interrupted()) {
        throw PGExceptionFactory.newQueryCancelledException();
      }
      if (includeReadyResponse) {
        ReadyResponse.send(
            connectionHandler.getConnectionMetadata().getOutputStream(),
            getBackendConnection().getConnectionState().getReadyResponseStatus());
      }
    } catch (Throwable exception) {
      recordException(exception);
      throw exception;
    } finally {
      connectionHandler.getConnectionMetadata().getOutputStream().flush();
      boolean shouldTrimToSize = messages.size() > MAX_BUFFER_CAPACITY;
      messages.clear();
      if (shouldTrimToSize) {
        messages.trimToSize();
        messages.ensureCapacity(DEFAULT_BUFFER_CAPACITY);
      }
      logger.log(Level.FINER, Logging.format("Flushing messages", Action.Finished));
      endSpan();
    }
  }

  private void addEvent(String event) {
    if (span != null) {
      span.addEvent(event);
    }
  }

  private void addEvent(String event, Attributes attributes) {
    if (span != null) {
      span.addEvent(event, attributes);
    }
  }

  private void endSpan() {
    if (span != null) {
      scope.close();
      span.end();
      span = null;
      backendConnection
          .getMetrics()
          .recordPGAdapterLatency(
              stopwatch.elapsed().toMillis(), backendConnection.getMetricAttributes());
    }
  }

  private void recordException(Throwable exception) {
    if (span != null) {
      span.setStatus(StatusCode.ERROR, exception.getMessage());
      span.recordException(exception);
    }
  }
}
