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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.Severity;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.cloud.spanner.pgadapter.wireoutput.TerminateResponse;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles a connection from a client to Spanner. This {@link ConnectionHandler} uses {@link
 * WireMessage} to receive and send all messages from and to the client, using the intermediate
 * representation {@link IntermediateStatement} that servers as a middle layer between Postgres and
 * Spanner.
 *
 * <p>Each {@link ConnectionHandler} is also a {@link Thread}. Although a TCP connection does not
 * necessarily need to have its own thread, this makes the implementation more straightforward.
 */
@InternalApi
public class ConnectionHandler extends Thread {
  private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final AtomicLong CONNECTION_HANDLER_ID_GENERATOR = new AtomicLong(0L);
  private static final String CHANNEL_PROVIDER_PROPERTY = "CHANNEL_PROVIDER";

  private final ProxyServer server;
  private final Socket socket;
  private final Map<String, IntermediatePreparedStatement> statementsMap = new HashMap<>();
  private final Map<String, IntermediatePortalStatement> portalsMap = new HashMap<>();
  private static final Map<Integer, IntermediateStatement> activeStatementsMap =
      new ConcurrentHashMap<>();
  private static final Map<Integer, Integer> connectionToSecretMapping = new ConcurrentHashMap<>();
  private volatile ConnectionStatus status = ConnectionStatus.UNAUTHENTICATED;
  private int connectionId;
  private final int secret;
  // Separate the following from the threat ID generator, since PG connection IDs are maximum
  //  32 bytes, and shouldn't be incremented on failed startups.
  private static final AtomicInteger incrementingConnectionId = new AtomicInteger(0);
  private ConnectionMetadata connectionMetadata;
  private WireMessage message;
  private Connection spannerConnection;

  ConnectionHandler(ProxyServer server, Socket socket) {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.secret = new SecureRandom().nextInt();
    setDaemon(true);
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s created for client %s",
                getName(), socket.getInetAddress().getHostAddress()));
  }

  @InternalApi
  public void connectToSpanner(String database) {
    OptionsMetadata options = getServer().getOptions();
    String uri =
        options.hasDefaultConnectionUrl()
            ? options.getDefaultConnectionUrl()
            : options.buildConnectionURL(database);
    if (uri.startsWith("jdbc:")) {
      uri = uri.substring("jdbc:".length());
    }
    uri = appendPropertiesToUrl(uri, getServer().getProperties());
    if (System.getProperty(CHANNEL_PROVIDER_PROPERTY) != null) {
      uri =
          uri
              + ";"
              + ConnectionOptions.CHANNEL_PROVIDER_PROPERTY_NAME
              + "="
              + System.getProperty(CHANNEL_PROVIDER_PROPERTY);
      // This forces the connection to use NoCredentials.
      uri = uri + ";usePlainText=true";
      try {
        Class.forName(System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      } catch (ClassNotFoundException e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown or invalid channel provider: "
                + System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      }
    }
    ConnectionOptions connectionOptions = ConnectionOptions.newBuilder().setUri(uri).build();
    Connection spannerConnection = connectionOptions.getConnection();
    try {
      // Note: Calling getDialect() will cause a SpannerException if the connection itself is
      // invalid, for example as a result of the credentials being wrong.
      if (spannerConnection.getDialect() != Dialect.POSTGRESQL) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format(
                "The database uses dialect %s. Currently PGAdapter only supports connections to PostgreSQL dialect databases. "
                    + "These can be created using https://cloud.google.com/spanner/docs/quickstart-console#postgresql",
                spannerConnection.getDialect()));
      }
    } catch (SpannerException e) {
      spannerConnection.close();
      throw e;
    }
    this.spannerConnection = spannerConnection;
  }

  private String appendPropertiesToUrl(String url, Properties info) {
    if (info == null || info.isEmpty()) {
      return url;
    }
    StringBuilder result = new StringBuilder(url);
    for (Entry<Object, Object> entry : info.entrySet()) {
      if (entry.getValue() != null && !"".equals(entry.getValue())) {
        result.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    return result.toString();
  }

  /**
   * Simple runner starts a loop which keeps taking inbound messages, processing them, sending them
   * to Spanner, getting a result, processing that result, and replying to the client (in that
   * order). Also instantiates input and output streams from the client and handles auth.
   */
  @Override
  public void run() {
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s starting for client %s",
                getName(), socket.getInetAddress().getHostAddress()));

    try (DataInputStream input =
            new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
        DataOutputStream output =
            new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()))) {
      if (!server.getOptions().disableLocalhostCheck()
          && !this.socket.getInetAddress().isAnyLocalAddress()
          && !this.socket.getInetAddress().isLoopbackAddress()) {
        handleError(
            output, new IllegalAccessException("This proxy may only be accessed from localhost."));
        return;
      }

      try {
        this.connectionMetadata = new ConnectionMetadata(input, output);
        this.message = this.server.recordMessage(BootstrapMessage.create(this));
        this.message.send();
        while (this.status == ConnectionStatus.UNAUTHENTICATED) {
          try {
            message.nextHandler();
            message.send();
          } catch (EOFException eofException) {
            // This indicates that the frontend terminated the connection before we got
            // authenticated. This is in most cases an indication that the frontend killed the
            // connection after having requested SSL and gotten an SSL denied message.
            this.status = ConnectionStatus.TERMINATED;
            break;
          }
        }
        while (this.status != ConnectionStatus.TERMINATED) {
          try {
            message.nextHandler();
            message.send();
          } catch (IllegalArgumentException | IllegalStateException | EOFException fatalException) {
            this.handleError(output, fatalException);
            this.status = ConnectionStatus.TERMINATED;
          } catch (Exception e) {
            this.handleError(output, e);
          }
        }
      } catch (Exception e) {
        this.handleError(output, e);
      }
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          e,
          () ->
              String.format(
                  "Exception on connection handler with ID %s for client %s: %s",
                  getName(), socket.getInetAddress().getHostAddress(), e));
    } finally {
      logger.log(
          Level.INFO, () -> String.format("Closing connection handler with ID %s", getName()));
      try {
        if (this.spannerConnection != null) {
          this.spannerConnection.close();
        }
        this.socket.close();
      } catch (SpannerException | IOException e) {
        logger.log(
            Level.WARNING,
            e,
            () ->
                String.format("Exception while closing connection handler with ID %s", getName()));
      }
      this.server.deregister(this);
      logger.log(
          Level.INFO, () -> String.format("Connection handler with ID %s closed", getName()));
    }
  }

  /** Called when a Terminate message is received. This closes this {@link ConnectionHandler}. */
  public void handleTerminate() {
    closeAllPortals();
    if (this.spannerConnection != null) {
      this.spannerConnection.close();
    }
    this.status = ConnectionStatus.TERMINATED;
  }

  /**
   * Terminates this connection at the request of the server. This is called if the server is
   * shutting down while the connection is still active.
   */
  void terminate() throws IOException {
    if (this.status != ConnectionStatus.TERMINATED) {
      handleTerminate();
    }
    if (!socket.isClosed()) {
      socket.close();
    }
  }

  /**
   * Takes an Exception Object and relates its results to the user within the client.
   *
   * @param e The exception to be related.
   * @throws IOException if there is some issue in the sending of the error messages.
   */
  private void handleError(DataOutputStream output, Exception e) throws Exception {
    logger.log(
        Level.WARNING,
        e,
        () -> String.format("Exception on connection handler with ID %s: %s", getName(), e));
    if (this.status == ConnectionStatus.TERMINATED) {
      new ErrorResponse(output, e, ErrorResponse.State.InternalError, Severity.FATAL).send();
      new TerminateResponse(output).send();
    } else if (this.status == ConnectionStatus.COPY_IN) {
      new ErrorResponse(output, e, ErrorResponse.State.InternalError).send();
    } else {
      this.status = ConnectionStatus.IDLE;
      new ErrorResponse(output, e, ErrorResponse.State.InternalError).send();
      new ReadyResponse(output, ReadyResponse.Status.IDLE).send();
    }
  }

  /** Closes portals and statements if the result of an execute was the end of a transaction. */
  public void cleanUp(IntermediateStatement statement) throws Exception {
    for (int index = 0; index < statement.getStatementCount(); index++) {
      if (!statement.isHasMoreData(index) && statement.isBound()) {
        statement.close(index);
      }
    }
    // TODO when we have transaction data from jdbcConnection, close all portals if done
  }

  /** Closes all named and unnamed portals on this connection. */
  private void closeAllPortals() {
    for (IntermediatePortalStatement statement : portalsMap.values()) {
      try {
        statement.close();
      } catch (Exception e) {
        logger.log(
            Level.SEVERE, e, () -> String.format("Unable to close portal: %s", e.getMessage()));
      }
    }
    this.portalsMap.clear();
    this.statementsMap.clear();
  }

  public IntermediatePortalStatement getPortal(String portalName) {
    if (!hasPortal(portalName)) {
      throw new IllegalStateException("Unregistered portal: " + portalName);
    }
    return this.portalsMap.get(portalName);
  }

  public void registerPortal(String portalName, IntermediatePortalStatement portal) {
    this.portalsMap.put(portalName, portal);
  }

  public void closePortal(String portalName) {
    if (!hasPortal(portalName)) {
      throw new IllegalStateException("Unregistered statement: " + portalName);
    }
    this.portalsMap.remove(portalName);
  }

  public boolean hasPortal(String portalName) {
    return this.portalsMap.containsKey(portalName);
  }

  /**
   * Add a currently executing statement to a buffer. This is only used in case a statement in
   * flight is cancelled. It must be saved separately, as a new connection is spawned to issue a
   * cancellation (as per Postgres protocol standard). This means some sort of IPC is required,
   * which in this case is a global Map.
   *
   * @param statement Currently executing statement to be saved.
   */
  public synchronized void addActiveStatement(IntermediateStatement statement) {
    activeStatementsMap.put(this.connectionId, statement);
  }

  /**
   * Remove a statement from the buffer if it is currently executing. For more information on this
   * use-case, read addActiveStatement comment.
   *
   * @param statement The statement to be removed.
   */
  public synchronized void removeActiveStatement(IntermediateStatement statement) {
    if (activeStatementsMap.get(this.connectionId) == statement) {
      activeStatementsMap.remove(this.connectionId);
    }
  }

  /**
   * To be used by a cancellation command to cancel a currently running statement, as contained in a
   * specific connection identified by connectionId. Since cancellation is a flimsy contract at
   * best, it is not imperative that the cancellation run, but it should be attempted nonetheless.
   *
   * @param connectionId The connection owhose statement must be cancelled.
   * @param secret The secret value linked to this connection. If it does not match, we cannot
   *     cancel.
   */
  public synchronized void cancelActiveStatement(int connectionId, int secret) {
    int expectedSecret = ConnectionHandler.connectionToSecretMapping.get(connectionId);
    if (secret != expectedSecret) {
      logger.log(
          Level.WARNING,
          () ->
              MessageFormat.format(
                  "User attempted to cancel a connection with the incorrect secret."
                      + "Connection: {}, Secret: {}, Expected Secret: {}",
                  connectionId,
                  secret,
                  expectedSecret));
      // Since the user does not accept a response, there is no need to except here: simply return.
      return;
    }
    if (activeStatementsMap.containsKey(connectionId)) {
      IntermediateStatement statement = activeStatementsMap.remove(connectionId);
      // We can mostly ignore the exception since cancel does not expect any result (positive or
      // otherwise)
      statement.getConnection().cancel();
    }
  }

  public IntermediatePreparedStatement getStatement(String statementName) {
    if (!hasStatement(statementName)) {
      throw new IllegalStateException("Unregistered statement: " + statementName);
    }
    return this.statementsMap.get(statementName);
  }

  public void registerStatement(String statementName, IntermediatePreparedStatement statement) {
    this.statementsMap.put(statementName, statement);
  }

  public void closeStatement(String statementName) {
    if (!hasStatement(statementName)) {
      throw new IllegalStateException("Unregistered statement: " + statementName);
    }
    this.statementsMap.remove(statementName);
  }

  public boolean hasStatement(String statementName) {
    return this.statementsMap.containsKey(statementName);
  }

  public ProxyServer getServer() {
    return this.server;
  }

  public Connection getSpannerConnection() {
    return this.spannerConnection;
  }

  public int getConnectionId() {
    if (this.connectionId == 0) {
      this.connectionId = ConnectionHandler.incrementingConnectionId.incrementAndGet();
      ConnectionHandler.connectionToSecretMapping.put(this.connectionId, this.secret);
    }
    return this.connectionId;
  }

  public int getSecret() {
    return this.secret;
  }

  public void setMessageState(WireMessage message) {
    this.message = this.server.recordMessage(message);
  }

  public ConnectionMetadata getConnectionMetadata() {
    return connectionMetadata;
  }

  public synchronized IntermediateStatement getActiveStatement() {
    return activeStatementsMap.get(this.connectionId);
  }

  public synchronized ConnectionStatus getStatus() {
    return status;
  }

  public synchronized void setStatus(ConnectionStatus status) {
    this.status = status;
  }

  /** Status of a {@link ConnectionHandler} */
  public enum ConnectionStatus {
    UNAUTHENTICATED(Status.IDLE),
    IDLE(Status.IDLE),
    TRANSACTION(Status.TRANSACTION),
    COPY_IN(Status.IDLE),
    TERMINATED(Status.IDLE),
    TRANSACTION_ABORTED(Status.FAILED);

    private final ReadyResponse.Status readyResponseStatus;

    ConnectionStatus(ReadyResponse.Status readyResponseStatus) {
      this.readyResponseStatus = readyResponseStatus;
    }

    public ReadyResponse.Status getReadyResponseStatus() {
      return this.readyResponseStatus;
    }
  }

  /**
   * PostgreSQL query mode (see also <a
   * href="https://www.postgresql.org/docs/current/protocol-flow.html">here</a>).
   */
  public enum QueryMode {
    SIMPLE,
    EXTENDED
  }
}
