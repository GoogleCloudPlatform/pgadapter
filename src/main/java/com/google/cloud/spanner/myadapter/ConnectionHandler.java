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

package com.google.cloud.spanner.myadapter;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.myadapter.error.MyException;
import com.google.cloud.spanner.myadapter.error.SQLState;
import com.google.cloud.spanner.myadapter.error.Severity;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.session.ProtocolStatus;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.wireinput.WireMessage;
import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLSocketFactory;

/**
 * Handles a connection from a client to Spanner. This {@link ConnectionHandler} uses {@link
 * WireMessage} to receive and send all messages from and to the client.
 *
 * <p>Each {@link ConnectionHandler} is also a {@link Thread}. Although a TCP connection does not
 * necessarily need to have its own thread, this makes the implementation more straightforward.
 */
@InternalApi
public class ConnectionHandler extends Thread {

  private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final AtomicLong CONNECTION_HANDLER_ID_GENERATOR = new AtomicLong(0L);

  private final SessionState sessionState;
  private final ProxyServer server;
  private Socket socket;
  private static final Map<Integer, ConnectionHandler> CONNECTION_HANDLERS =
      new ConcurrentHashMap<>();
  private final int connectionId;
  // Separate the following from the threat ID generator, since PG connection IDs are maximum
  //  32 bytes, and shouldn't be incremented on failed startups.
  private static final AtomicInteger incrementingConnectionId = new AtomicInteger(0);
  private ConnectionMetadata connectionMetadata;
  private int sequenceNumber;
  private BackendConnection backendConnection;
  private final OptionsMetadata options;

  public WireProtocolHandler getWireHandler() {
    return wireHandler;
  }

  private WireProtocolHandler wireHandler;

  ConnectionHandler(ProxyServer server, Socket socket) {
    this(server, socket, null);
  }

  /** Constructor only for testing. */
  @VisibleForTesting
  ConnectionHandler(ProxyServer server, Socket socket, Connection spannerConnection) {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.connectionId = incrementingConnectionId.incrementAndGet();
    CONNECTION_HANDLERS.put(this.connectionId, this);
    setDaemon(true);
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s created for client %s",
                getName(), socket.getInetAddress().getHostAddress()));
    this.options = server.getOptions();
    this.backendConnection =
        new BackendConnection(options, server.getProperties(), spannerConnection);
    this.sessionState = new SessionState();
  }

  void createSSLSocket() throws IOException {
    this.socket =
        ((SSLSocketFactory) SSLSocketFactory.getDefault()).createSocket(socket, null, true);
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
    runConnection();
  }

  /**
   * Starts listening for incoming messages on the network socket. Returns RESTART_WITH_SSL if the
   * listening process should be restarted with SSL.
   */
  private void runConnection() {
    try (ConnectionMetadata connectionMetadata =
        new ConnectionMetadata(this.socket.getInputStream(), this.socket.getOutputStream())) {
      this.connectionMetadata = connectionMetadata;
      this.wireHandler =
          new WireProtocolHandler(connectionMetadata, sessionState, backendConnection, options);

      try {
        wireHandler.run();
      } catch (MyException myException) {
        this.handleError(myException);
      } catch (Exception exception) {
        this.handleError(
            MyException.newBuilder(exception)
                .setSeverity(Severity.FATAL)
                .setSQLState(SQLState.InternalError)
                .build());
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
      if (sessionState.getProtocolStatus() != ProtocolStatus.RESTART_WITH_SSL) {
        logger.log(
            Level.INFO, () -> String.format("Closing connection handler with ID %s", getName()));
        try {
          if (this.backendConnection != null) {
            this.backendConnection.terminate();
          }
          this.socket.close();
        } catch (SpannerException | IOException e) {
          logger.log(
              Level.WARNING,
              e,
              () ->
                  String.format(
                      "Exception while closing connection handler with ID %s", getName()));
        }
        this.server.deregister(this);
        logger.log(
            Level.INFO, () -> String.format("Connection handler with ID %s closed", getName()));
      }
    }
  }

  /** Called when a Terminate message is received. This closes this {@link ConnectionHandler}. */
  public void handleTerminate() {
    synchronized (this) {
      backendConnection.terminate();
      this.sessionState.setProtocolStatus(ProtocolStatus.TERMINATED);
      CONNECTION_HANDLERS.remove(this.connectionId);
    }
  }

  /**
   * Terminates this connection at the request of the server. This is called if the server is
   * shutting down while the connection is still active.
   */
  void terminate() {
    handleTerminate();
    try {
      if (!socket.isClosed()) {
        socket.close();
      }
    } catch (IOException exception) {
      logger.log(
          Level.WARNING,
          exception,
          () ->
              String.format(
                  "Failed to close connection handler with ID %s: %s", getName(), exception));
    }
  }

  /**
   * Takes an Exception Object and relates its results to the user within the client.
   *
   * @param exception The exception to be related.
   * @throws IOException if there is some issue in the sending of the error messages.
   */
  void handleError(MyException exception) throws Exception {
    logger.log(
        Level.WARNING,
        exception,
        () ->
            String.format("Exception on connection handler with ID %s: %s", getName(), exception));
    DataOutputStream output = getConnectionMetadata().getOutputStream();
  }

  public ProxyServer getServer() {
    return this.server;
  }

  public ConnectionMetadata getConnectionMetadata() {
    return connectionMetadata;
  }
}
