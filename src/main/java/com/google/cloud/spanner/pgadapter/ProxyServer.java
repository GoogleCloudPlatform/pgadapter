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

import com.google.api.core.AbstractApiService;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse.Severity;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.collect.ImmutableList;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

/**
 * The proxy server listens for incoming client connections and starts a new {@link
 * ConnectionHandler} for each incoming connection.
 */
public class ProxyServer extends AbstractApiService {

  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());
  private final OptionsMetadata options;
  private final Properties properties;
  private final List<ConnectionHandler> handlers = Collections.synchronizedList(new LinkedList<>());

  /**
   * Latch that is closed when the TCP server has started. We need this to know the exact port that
   * the TCP socket was assigned, so we can assign the same port number to the Unix domain socket.
   */
  private final CountDownLatch tcpStartedLatch = new CountDownLatch(1);
  /**
   * List of server sockets accepting connections. It currently only contains one TCP socket and
   * optionally one Unix domain socket, but could in theory be expanded to contain multiple sockets
   * of each type.
   */
  private final List<ServerSocket> serverSockets = Collections.synchronizedList(new LinkedList<>());

  private int localPort;

  /** The server will keep track of all messages it receives if it started in DEBUG mode. */
  private static final int MAX_DEBUG_MESSAGES = 100_000;

  private final boolean debugMode;
  private final ConcurrentLinkedQueue<WireMessage> debugMessages = new ConcurrentLinkedQueue<>();
  private final AtomicInteger debugMessageCount = new AtomicInteger();

  /**
   * Instantiates the ProxyServer from CLI-gathered metadata.
   *
   * @param optionsMetadata Resulting metadata from CLI.
   */
  public ProxyServer(OptionsMetadata optionsMetadata) {
    this.options = optionsMetadata;
    this.localPort = optionsMetadata.getProxyPort();
    this.properties = new Properties();
    this.debugMode = optionsMetadata.isDebugMode();
    addConnectionProperties();
  }

  /**
   * Instantiates the ProxyServer from metadata and properties. For use with in-process invocations.
   *
   * @param optionsMetadata Resulting metadata from CLI.
   * @param properties Properties for specificying additional information to JDBC like an external
   *     channel provider (see ConnectionOptions in Java Spanner client library for more details on
   *     supported properties).
   */
  public ProxyServer(OptionsMetadata optionsMetadata, Properties properties) {
    this.options = optionsMetadata;
    this.localPort = optionsMetadata.getProxyPort();
    this.properties = properties;
    this.debugMode = optionsMetadata.isDebugMode();
    addConnectionProperties();
  }

  private void addConnectionProperties() {
    for (Map.Entry<String, String> entry : options.getPropertyMap().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
  }

  /** Starts the server by running the thread runnable and setting status. */
  public void startServer() {
    startAsync();
    awaitRunning();
    logger.log(Level.INFO, () -> String.format("Server started on port %d", getLocalPort()));
  }

  interface ServerRunnable {
    void run(CountDownLatch startupLatch, CountDownLatch stoppedLatch)
        throws IOException, InterruptedException;
  }

  @Override
  protected void doStart() {
    ImmutableList<ServerRunnable> serverRunnables;
    if (options.isDomainSocketEnabled()) {
      serverRunnables = ImmutableList.of(this::runTcpServer, this::runDomainSocketServer);
    } else {
      serverRunnables = ImmutableList.of(this::runTcpServer);
    }
    CountDownLatch startupLatch = new CountDownLatch(serverRunnables.size());
    CountDownLatch stoppedLatch = new CountDownLatch(serverRunnables.size());
    for (ServerRunnable server : serverRunnables) {
      Thread listenerThread =
          new Thread("spanner-postgres-adapter-proxy-listener") {
            @Override
            public void run() {
              try {
                server.run(startupLatch, stoppedLatch);
              } catch (Exception exception) {
                logger.log(
                    Level.WARNING,
                    exception,
                    () ->
                        String.format(
                            "Server on port %s stopped by exception: %s",
                            getLocalPort(), exception));
              }
            }
          };
      listenerThread.start();
    }
    try {
      if (startupLatch.await(30L, TimeUnit.SECONDS)) {
        notifyStarted();
      } else {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.DEADLINE_EXCEEDED, "The server did not start in a timely fashion.");
      }
    } catch (InterruptedException interruptedException) {
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    }
  }

  @Override
  protected void doStop() {
    for (ServerSocket serverSocket : this.serverSockets) {
      try {
        logger.log(
            Level.INFO, () -> String.format("Server on socket %s is stopping", serverSocket));
        serverSocket.close();
        logger.log(
            Level.INFO, () -> String.format("Server socket on socket %s closed", serverSocket));
      } catch (IOException exception) {
        logger.log(
            Level.WARNING,
            exception,
            () -> String.format("Closing server socket %s failed: %s", serverSocket, exception));
      }
    }
    for (ConnectionHandler handler : this.handlers) {
      handler.terminate();
    }
    notifyStopped();
  }

  /** Safely stops the server (iff started), closing specific socket and cleaning up. */
  public void stopServer() {
    stopAsync();
    awaitTerminated();
  }

  /**
   * Thread logic: opens the TCP listening socket and instantiates the connection handler.
   *
   * @throws IOException if ServerSocket cannot start.
   */
  void runTcpServer(CountDownLatch startupLatch, CountDownLatch stoppedLatch) throws IOException {
    ServerSocket tcpSocket =
        new ServerSocket(this.options.getProxyPort(), this.options.getMaxBacklog());
    this.serverSockets.add(tcpSocket);
    this.localPort = tcpSocket.getLocalPort();
    tcpStartedLatch.countDown();
    runServer(tcpSocket, startupLatch, stoppedLatch);
  }

  void runDomainSocketServer(CountDownLatch startupLatch, CountDownLatch stoppedLatch)
      throws IOException, InterruptedException {
    // Wait until the TCP server has started if it is using a dynamic port, so we can get the port
    // number it is using.
    if (options.getProxyPort() == 0 && !tcpStartedLatch.await(30L, TimeUnit.SECONDS)) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.DEADLINE_EXCEEDED, "Timeout while waiting for TCP server to start");
    }
    File tempDir = new File(this.options.getSocketFile(getLocalPort()));
    try {
      if (tempDir.getParentFile() != null && !tempDir.getParentFile().exists()) {
        tempDir.mkdirs();
      }
      AFUNIXServerSocket domainSocket = AFUNIXServerSocket.newInstance();
      domainSocket.bind(AFUNIXSocketAddress.of(tempDir), this.options.getMaxBacklog());
      this.serverSockets.add(domainSocket);
      runServer(domainSocket, startupLatch, stoppedLatch);
    } catch (SocketException socketException) {
      logger.log(
          Level.SEVERE,
          String.format(
              "Failed to bind to Unix domain socket. Please verify that the user running PGAdapter has write permission for file %s",
              tempDir),
          socketException);
      startupLatch.countDown();
    }
  }

  void runServer(
      ServerSocket serverSocket, CountDownLatch startupLatch, CountDownLatch stoppedLatch)
      throws IOException {
    startupLatch.countDown();
    awaitRunning();
    try {
      while (isRunning()) {
        Socket socket = serverSocket.accept();
        try {
          createConnectionHandler(socket);
        } catch (SpannerException exception) {
          handleConnectionError(exception, socket);
        }
      }
    } catch (SocketException e) {
      // This is a normal exception, as this will occur when Server#stopServer() is called.
      logger.log(
          Level.INFO,
          () ->
              String.format(
                  "Socket exception on socket %s: %s. This is normal when the server is stopped.",
                  serverSocket, e));
    } finally {
      logger.log(Level.INFO, () -> String.format("Socket %s stopped", serverSocket));
      stoppedLatch.countDown();
    }
  }

  /**
   * Sends a message to the client that the connection could not be established.
   *
   * @param exception The exception that caused the connection request to fail.
   * @param socket The socket that was created for the connection.
   */
  private void handleConnectionError(SpannerException exception, Socket socket) {
    logger.log(
        Level.SEVERE,
        exception,
        () ->
            String.format(
                "Something went wrong in establishing a Spanner connection: %s",
                exception.getMessage()));
    try {
      DataOutputStream output =
          new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      new ErrorResponse(output, exception, ErrorResponse.State.ConnectionException, Severity.FATAL)
          .send();
      output.flush();
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          e,
          () -> String.format("Failed to send fatal error message to client: %s", e.getMessage()));
    }
  }

  /**
   * Creates and runs the {@link ConnectionHandler}, saving an instance of it locally.
   *
   * @param socket The socket the {@link ConnectionHandler} will read from.
   * @throws SpannerException if the {@link ConnectionHandler} is unable to connect to Cloud Spanner
   *     or if the dialect of the database is not PostgreSQL.
   */
  void createConnectionHandler(Socket socket) {
    ConnectionHandler handler = new ConnectionHandler(this, socket);
    register(handler);
    handler.start();
  }

  /**
   * Saves the handler locally
   *
   * @param handler The handler currently in use.
   */
  private void register(ConnectionHandler handler) {
    this.handlers.add(handler);
  }

  /**
   * Revokes the currently saved handler.
   *
   * @param handler The handler to revoke.
   */
  void deregister(ConnectionHandler handler) {
    this.handlers.remove(handler);
  }

  public OptionsMetadata getOptions() {
    return this.options;
  }

  /** @return the JDBC connection properties that are used by this server */
  public Properties getProperties() {
    return (Properties) this.properties.clone();
  }

  /** @return the current number of connections. */
  public int getNumberOfConnections() {
    return this.handlers.size();
  }

  /** @return the local TCP port that this server is using. */
  public int getLocalPort() {
    return localPort;
  }

  @Override
  public String toString() {
    return String.format("ProxyServer[port: %d]", getLocalPort());
  }

  ConcurrentLinkedQueue<WireMessage> getDebugMessages() {
    return debugMessages;
  }

  void clearDebugMessages() {
    synchronized (debugMessages) {
      debugMessages.clear();
      debugMessageCount.set(0);
    }
  }

  WireMessage recordMessage(WireMessage message) {
    if (debugMode) {
      if (debugMessageCount.get() >= MAX_DEBUG_MESSAGES) {
        throw new IllegalStateException(
            "Received too many debug messages. Did you turn on DEBUG mode by accident?");
      }
      debugMessages.add(message);
      debugMessageCount.incrementAndGet();
    }
    return message;
  }

  /**
   * The PostgreSQL wire protocol can send data in both binary and text format. When using text
   * format, the {@link Server} will normally send output back to the client using a format
   * understood by PostgreSQL clients. If you are using the server with a text-only client that does
   * not try to interpret the data that is returned by the server, such as for example psql, then it
   * is advisable to use Cloud Spanner formatting. The server will then return all data in a format
   * understood by Cloud Spanner.
   *
   * <p>The default format used by the server is {@link DataFormat#POSTGRESQL_TEXT}.
   */
  public enum DataFormat {
    /**
     * Data is returned to the client in binary form. This is the most compact format, but it is not
     * supported by all clients for all data types. Only when the client specifically requests that
     * the data should be returned in binary format, will the server do so.
     */
    POSTGRESQL_BINARY((short) 1),
    /**
     * The default format. Data is returned to the client in a format that PostgreSQL clients should
     * be able to understand and stringParse. Use this format if you are using the {@link Server}
     * with a client that tries to interpret the data that is returned by the server, such as for
     * example the PostgreSQL JDBC driver.
     */
    POSTGRESQL_TEXT((short) 0),
    /**
     * Data is returned to the client in Cloud Spanner format. Use this format if you are using the
     * server with a text-only client, such as psql, that does not try to interpret and stringParse
     * the data that is returned.
     */
    SPANNER((short) 0);

    /**
     * The internal code used by the PostgreSQL wire protocol to determine whether the data should
     * be interpreted as text or binary.
     */
    private final short code;

    DataFormat(short code) {
      this.code = code;
    }

    public static DataFormat getDataFormat(
        int index, IntermediateStatement statement, QueryMode mode, OptionsMetadata options) {
      if (options.isBinaryFormat()) {
        return DataFormat.POSTGRESQL_BINARY;
      }
      if (mode == QueryMode.SIMPLE) {
        // Simple query mode is always text.
        return DataFormat.fromTextFormat(options.getTextFormat());
      } else {
        short resultFormatCode = statement == null ? 0 : statement.getResultFormatCode(index);
        return DataFormat.byCode(resultFormatCode, options.getTextFormat());
      }
    }

    public static DataFormat fromTextFormat(TextFormat textFormat) {
      switch (textFormat) {
        case POSTGRESQL:
          return POSTGRESQL_TEXT;
        case SPANNER:
          return DataFormat.SPANNER;
        default:
          throw new IllegalArgumentException();
      }
    }

    public static DataFormat byCode(short code, TextFormat textFormat) {
      return code == 0 ? fromTextFormat(textFormat) : POSTGRESQL_BINARY;
    }

    public short getCode() {
      return code;
    }
  }
}
