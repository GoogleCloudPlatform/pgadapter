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

import com.google.cloud.spanner.pgadapter.PGWireProtocol.Status;
import com.google.cloud.spanner.pgadapter.metadata.DescribePortalMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeStatementMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles a connection from a client to Spanner. This {@link ConnectionHandler} has a {@link
 * PGWireProtocol} that is responsible for sending all messages from and to the client, using the
 * intermediate representation {@link IntermediateStatement} that servers as a middle layer between
 * Postgres and Spanner.
 *
 * Each {@link ConnectionHandler} is also a {@link Thread}. Although a TCP connection does not
 * necessarily need to have its own thread, this makes the implementation more straightforward.
 */
public class ConnectionHandler extends Thread {

  private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final AtomicLong CONNECTION_HANDLER_ID_GENERATOR = new AtomicLong(0L);
  private final ProxyServer server;
  private final Socket socket;
  private final Map<String, IntermediatePreparedStatement> statementsMap = new HashMap<>();
  private final Map<String, IntermediatePortalStatement> portalsMap = new HashMap<>();
  private volatile ConnectionStatus status = ConnectionStatus.UNAUTHENTICATED;
  private PGWireProtocol wire;
  private Connection jdbcConnection;

  ConnectionHandler(ProxyServer server, Socket socket) throws SQLException {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.jdbcConnection = DriverManager.getConnection(server.getOptions().getConnectionURL());
    setDaemon(true);
    logger.log(Level.INFO, "Connection handler with ID {0} created for client {1}",
        new Object[]{getName(), socket.getInetAddress().getHostAddress()});
  }

  /**
   * Simple runner starts a loop which keeps taking inbound messages, processing them, sending them
   * to Spanner, getting a result, processing that result, and replying to the client (in that
   * order). Also instantiates input and output streams from the client and handles auth.
   */
  @Override
  public void run() {
    logger.log(Level.INFO, "Connection handler with ID {0} starting for client {1}",
        new Object[]{getName(), socket.getInetAddress().getHostAddress()});

    try (
        DataInputStream input =
            new DataInputStream(new BufferedInputStream(this.socket.getInputStream()));
        DataOutputStream output =
            new DataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()))) {
      this.wire = new PGWireProtocol(this, input, output);
      if (!this.socket.getInetAddress().isAnyLocalAddress() &&
          !this.socket.getInetAddress().isLoopbackAddress()) {
        handleError(new IllegalAccessException("This proxy may only be accessed from localhost."));
        return;
      }
      if (this.wire.dontAuthenticate() || this.wire.doAuthenticate()) {
        logger.log(Level.INFO, "Connection handler with ID {0} authenticated for client {1}",
            new Object[]{getName(), this.socket.getInetAddress().getHostAddress()});
        this.status = ConnectionStatus.IDLE;
        while (this.status != ConnectionStatus.TERMINATED) {
          logger.log(Level.FINEST, "Connection handler with ID {0} waiting for messages",
              getName());
          WireMessage message;
          try {
            message = WireMessage.create(this, input);
            logger.log(Level.FINEST, "Connection handler with ID {0} received message {1}",
                new Object[]{getName(), message});
          } catch (Exception e) {
            this.handleError(e);
            continue;
          }
          try {
            message.send();
            logger.log(Level.FINEST, "Connection handler with ID {0} executed message {1}",
                new Object[]{getName(), message});
          } catch (Exception e) {
            this.handleError(e);
          }
        }
      } else {
        logger.log(Level.INFO,
            "Authentication failed on connection handler with ID {0} for client {1}",
            new Object[]{getName(), socket.getInetAddress().getHostAddress()});
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Exception on connection handler with ID {0} for client {1}: {2}",
          new Object[]{getName(), socket.getInetAddress().getHostAddress(), e});
    } finally {
      logger.log(Level.INFO, "Closing connection handler with ID {0}", getName());
      try {
        if (this.jdbcConnection != null) {
          this.jdbcConnection.close();
        }
        this.socket.close();
      } catch (SQLException | IOException e) {
        logger.log(Level.WARNING, "Exception while closing connection handler with ID {0}",
            getName());
      }
      this.server.deregister(this);
      logger.log(Level.INFO, "Connection handler with ID {0} closed", getName());
    }
  }

  /**
   * Called when a Parse message is received
   **/
  public void handleParse() throws IOException {
    this.wire.sendParseComplete();
  }

  /**
   * Called when a Bind message is received.
   */
  public void handleBind() throws IOException {
    this.wire.sendBindComplete();
  }

  /**
   * Handles the return of a query message to the client. Specifically sends the row description as
   * well as the row contents themselves to the client.
   *
   * @param statement {@link IntermediateStatement} containing the current representation.
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleQuery(IntermediateStatement statement) throws Exception {
    if (statement.hasException()) {
      this.handleError(statement.getException());
    } else {
      if (statement.containsResultSet()) {
        this.wire.sendRowDescription(statement,
            statement.getStatementResult().getMetaData(),
            this.server.getOptions().getTextFormat(),
            QueryMode.SIMPLE);
      }
      sendSpannerResult(statement, QueryMode.SIMPLE, 0L);
      this.wire.sendReadyForQuery(Status.IDLE);
    }
    cleanUp(statement);
  }

  /**
   * Called when a describe message of type 'P' is received.
   *
   * @param statement The statement to be described.
   * @param describe The metadata relating to the statement.
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleDescribePortal(IntermediateStatement statement, DescribePortalMetadata describe)
      throws Exception {
    if (statement.hasException()) {
      this.handleError(statement.getException());
    } else {
      this.wire.sendRowDescription(statement, describe.getMetadata(),
          this.server.getOptions().getTextFormat(), QueryMode.EXTENDED);
    }
  }

  /**
   * Called when a describe message of type 'S' is received.
   *
   * @param describe The metadata relating to the statement.
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleDescribeStatement(DescribeStatementMetadata describe) throws Exception {
    this.wire.sendParameterDescription(describe.getMetadata());
    this.wire.sendNoData();
  }

  /**
   * Called when an execute message is received.
   *
   * @param statement The statement to execute.
   * @param maxRows The maximum number of rows to send back.
   * @throws Exception if sending the message back to the client causes an error.
   */
  public void handleExecute(IntermediateStatement statement, int maxRows) throws Exception {
    if (statement.hasException()) {
      this.handleError(statement.getException());
    } else {
      sendSpannerResult(statement, QueryMode.EXTENDED, maxRows);
    }
    cleanUp(statement);
  }

  /**
   * Called when a close message is received.
   */
  public void handleClose() throws Exception {
    this.wire.sendCloseComplete();

  }

  /**
   * Called when a Sync message is received.
   */
  public void handleSync() throws IOException {
    this.wire.sendReadyForQuery(Status.IDLE);
  }

  /**
   * Called when a Flush message is received.
   */
  public void handleFlush() throws IOException {
    this.wire.sendReadyForQuery(Status.IDLE);
  }

  /**
   * Called when a Terminate message is received. This closes this {@link ConnectionHandler}.
   */
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
  private void handleError(Exception e) throws IOException {
    logger.log(Level.FINEST,
        "Exception on connection handler with ID {0}: {2}",
        new Object[]{getName(), e});
    this.wire.sendError(e);
    this.wire.sendReadyForQuery(Status.IDLE);
  }

  /**
   * Sends the result of an execute or query to the client. The type of message depends on the type
   * of result of the statement. This method may also be called multiple times for one result if the
   * client has set a max number of rows to fetch for each execute message. The {@link
   * IntermediateStatement} will cache the result in between calls and continue serving rows from
   * the position it was left off after the last execute message.
   */
  private boolean sendSpannerResult(IntermediateStatement statement, QueryMode mode, long maxRows)
      throws IOException, SQLException {
    logger.log(Level.FINEST, "Sending result of statement {0}", statement.getStatement());
    switch (statement.getResultType()) {
      case NO_RESULT:
        this.wire.sendCommandComplete(statement.getCommand());
        return false;
      case RESULT_SET:
        SendResultSetState state = this.wire.sendResultSet(statement, mode, maxRows);
        statement.setHasMoreData(state.hasMoreRows);
        if (state.hasMoreRows) {
          this.wire.sendPortalSuspended();
        } else {
          statement.getStatementResult().close();
          this.wire.sendCommandComplete("SELECT " + state.rowsSent);
        }
        return state.hasMoreRows;
      case UPDATE_COUNT:
        String cmd = statement.getCommand();
        if ("INSERT".equals(cmd)) {
          // INSERT statements require both an OID and an update count to be returned.
          // Cloud Spanner has no equivalent for this, so the returned OID is always zero.
          cmd = cmd + " 0";
        }
        wire.sendCommandComplete(cmd + " " + statement.getUpdateCount());
        return false;
      default:
        throw new IllegalStateException(
            "Unknown result type: " + statement.getResultType());
    }
  }

  /**
   * Closes portals and statements if the result of an execute was the end of a transaction.
   */
  private void cleanUp(IntermediateStatement statement) throws Exception {
    if (!statement.isHasMoreData() && statement.isBound()) {
      statement.close();
    }
    // TODO when we have transaction data from jdbcConnection, close all portals if done
  }

  /**
   * Closes all named and unnamed portals on this connection.
   */
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

  /**
   * Status of a {@link ConnectionHandler}
   */
  private enum ConnectionStatus {
    UNAUTHENTICATED, IDLE, TERMINATED
  }

  /**
   * PostgreSQL query mode (see also
   * <a href="https://www.postgresql.org/docs/current/protocol-flow.html">
   * here</a>).
   */
  enum QueryMode {
    SIMPLE, EXTENDED
  }

  /**
   * The state of a result after (a part of it) has been sent to the client.
   */
  static class SendResultSetState {

    private final long rowsSent;
    private final boolean hasMoreRows;

    SendResultSetState(long rowsSent, boolean hasMoreRows) {
      this.rowsSent = rowsSent;
      this.hasMoreRows = hasMoreRows;
    }
  }


}
