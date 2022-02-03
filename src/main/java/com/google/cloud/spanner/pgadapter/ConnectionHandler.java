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

import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
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
public class ConnectionHandler extends Thread {

  private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final AtomicLong CONNECTION_HANDLER_ID_GENERATOR = new AtomicLong(0L);
  private final ProxyServer server;
  private final Socket socket;
  private final Map<String, IntermediatePreparedStatement> statementsMap = new HashMap<>();
  private final Map<String, IntermediatePortalStatement> portalsMap = new HashMap<>();
  private static volatile Map<Integer, IntermediateStatement> activeStatementsMap = new HashMap<>();
  private static final Map<Integer, Integer> connectionToSecretMapping = new HashMap<>();
  private volatile ConnectionStatus status = ConnectionStatus.UNAUTHENTICATED;
  private int connectionId;
  private final int secret;
  // Separate the following from the threat ID generator, since PG connection IDs are maximum
  //  32 bytes, and shouldn't be incremented on failed startups.
  private static AtomicInteger incrementingConnectionId = new AtomicInteger(0);
  private ConnectionMetadata connectionMetadata;
  private WireMessage message;
  private Connection jdbcConnection;

  ConnectionHandler(ProxyServer server, Socket socket) throws SQLException {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.secret = new SecureRandom().nextInt();
    this.jdbcConnection =
        DriverManager.getConnection(server.getOptions().getConnectionURL(), server.getProperties());
    setDaemon(true);
    logger.log(
        Level.INFO,
        "Connection handler with ID {0} created for client {1}",
        new Object[] {getName(), socket.getInetAddress().getHostAddress()});
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
        "Connection handler with ID {0} starting for client {1}",
        new Object[] {getName(), socket.getInetAddress().getHostAddress()});

    try (DataInputStream input =
            new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
        DataOutputStream output =
            new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream())); ) {
      if (!server.getOptions().disableLocalhostCheck()
          && !this.socket.getInetAddress().isAnyLocalAddress()
          && !this.socket.getInetAddress().isLoopbackAddress()) {
        handleError(
            output, new IllegalAccessException("This proxy may only be accessed from localhost."));
        return;
      }

      try {
        this.connectionMetadata = new ConnectionMetadata(input, output);
        this.message = BootstrapMessage.create(this);
        this.message.send();
        while (this.status != ConnectionStatus.TERMINATED) {
          try {
            message.nextHandler();
            message.send();
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
          "Exception on connection handler with ID {0} for client {1}: {2}",
          new Object[] {getName(), socket.getInetAddress().getHostAddress(), e});
    } finally {
      logger.log(Level.INFO, "Closing connection handler with ID {0}", getName());
      try {
        if (this.jdbcConnection != null) {
          this.jdbcConnection.close();
        }
        this.socket.close();
      } catch (SQLException | IOException e) {
        logger.log(
            Level.WARNING, "Exception while closing connection handler with ID {0}", getName());
      }
      this.server.deregister(this);
      logger.log(Level.INFO, "Connection handler with ID {0} closed", getName());
    }
  }

  /** Called when a Terminate message is received. This closes this {@link ConnectionHandler}. */
  public void handleTerminate() throws Exception {
    closeAllPortals();
    this.jdbcConnection.close();
    this.status = ConnectionStatus.TERMINATED;
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
        "Exception on connection handler with ID {0}: {1}",
        new Object[] {getName(), e});
    new ErrorResponse(output, e, ErrorResponse.State.InternalError).send();
    new ReadyResponse(output, ReadyResponse.Status.IDLE).send();
  }

  /** Closes portals and statements if the result of an execute was the end of a transaction. */
  public void cleanUp(IntermediateStatement statement) throws Exception {
    for (int index = 0; index < statement.getStatementCount(); index++) {
      if (!statement.isHasMoreData(index) && statement.isBound()) {
        statement.close();
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
        logger.log(Level.SEVERE, "Unable to close portal: {0}", e.getMessage());
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
   * @throws Exception If Cancellation fails.
   */
  public synchronized void cancelActiveStatement(int connectionId, int secret) throws Exception {
    int expectedSecret = ConnectionHandler.connectionToSecretMapping.get(connectionId);
    if (secret != expectedSecret) {
      logger.log(
          Level.WARNING,
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
      statement.getStatement().cancel();
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

  public Connection getJdbcConnection() {
    return this.jdbcConnection;
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
    this.message = message;
  }

  public ConnectionMetadata getConnectionMetadata() {
    return connectionMetadata;
  }

  /** Status of a {@link ConnectionHandler} */
  private enum ConnectionStatus {
    UNAUTHENTICATED,
    IDLE,
    TERMINATED
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
