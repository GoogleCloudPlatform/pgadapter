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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;

/**
 * The proxy server listens for incoming client connections and starts a new {@link
 * ConnectionHandler} for each incoming connection.
 */
public class ProxyServer extends Thread {

  private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());
  private final OptionsMetadata options;
  private final Properties properties;
  private final List<ConnectionHandler> handlers = new LinkedList<>();

  @GuardedBy("itself")
  private volatile ServerStatus status = ServerStatus.NEW;

  private ServerSocket serverSocket;

  /**
   * Instantiates the ProxyServer from CLI-gathered metadata.
   *
   * @param optionsMetadata Resulting metadata from CLI.
   */
  public ProxyServer(OptionsMetadata optionsMetadata) {
    super("spanner-postgres-adapter-proxy-port-" + optionsMetadata.getProxyPort());
    this.options = optionsMetadata;
    this.properties = new Properties();
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
    super("spanner-postgres-adapter-proxy-port-" + optionsMetadata.getProxyPort());
    this.options = optionsMetadata;
    this.properties = properties;
    addConnectionProperties();
  }

  private void addConnectionProperties() {
    for (Map.Entry<String, String> entry : options.getPropertyMap().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
  }

  /** Starts the server by running the thread runnable and setting status. */
  public void startServer() {
    synchronized (this.status) {
      Preconditions.checkState(this.status == ServerStatus.NEW);
      logger.log(
          Level.INFO,
          "Server is starting on port {0}",
          String.valueOf(this.options.getProxyPort()));
      this.status = ServerStatus.STARTING;
      start();
      // TODO: Consider changing the server to use ApiService, so we don't have to manage the state
      //  of the server manually.
      while (this.status == ServerStatus.STARTING) {
        Thread.yield();
      }
      logger.log(Level.INFO, "Server started on port {0}", String.valueOf(getLocalPort()));
    }
  }

  /** Safely stops the server (iff started), closing specific socket and cleaning up. */
  public void stopServer() {
    synchronized (status) {
      Preconditions.checkState(status == ServerStatus.STARTED, "Server is not in state Started");
      try {
        logger.log(
            Level.INFO,
            "Server on port {0} is stopping",
            String.valueOf(this.options.getProxyPort()));
        this.status = ServerStatus.STOPPING;
        this.serverSocket.close();
        logger.log(
            Level.INFO,
            "Server socket on port {0} closed",
            String.valueOf(this.options.getProxyPort()));
      } catch (IOException e) {
        logger.log(
            Level.WARNING,
            "Closing server socket on port {0} failed: {1}",
            new Object[] {String.valueOf(this.options.getProxyPort()), e});
      }
    }
  }

  @Override
  public void run() {
    try {
      runServer();
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          "Server on port {0} stopped by exception: {1}",
          new Object[] {this.options.getProxyPort(), e});
    }
  }

  /**
   * Thread logic: opens the listening socket and instantiates the connection handler.
   *
   * @throws IOException if ServerSocket cannot start.
   */
  void runServer() throws IOException {
    this.serverSocket = new ServerSocket(this.options.getProxyPort());
    this.status = ServerStatus.STARTED;
    try {
      while (this.status == ServerStatus.STARTED) {
        Socket socket = this.serverSocket.accept();
        try {
          createConnectionHandler(socket);
        } catch (SpannerException e) {
          logger.log(
              Level.SEVERE,
              "Something went wrong in establishing a Spanner connection: {0}",
              e.getMessage());
          socket.close();
        }
      }
    } catch (SocketException e) {
      // This is a normal exception, as this will occur when Server#stopServer() is called.
      logger.log(
          Level.INFO,
          "Socket exception on port {0}: {1}. This is normal when the server is stopped.",
          new Object[] {getLocalPort(), e});
    } catch (SpannerException e) {
      logger.log(
          Level.SEVERE,
          "Something went wrong in establishing a Spanner connection: {0}",
          e.getMessage());
    } finally {
      this.status = ServerStatus.STOPPED;
      logger.log(Level.INFO, "Socket on port {0} stopped", getLocalPort());
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
    // Note: Calling getDialect() will cause a SpannerException if the connection itself is
    // invalid, for example as a result of the credentials being wrong.
    if (handler.getSpannerConnection().getDialect() != Dialect.POSTGRESQL) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "The database uses dialect %s. Currently PGAdapter only supports connections to PostgreSQL dialect databases. "
                  + "These can be created using https://cloud.google.com/spanner/docs/quickstart-console#postgresql",
              handler.getSpannerConnection().getDialect()));
    }
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

  public Properties getProperties() {
    return this.properties;
  }

  public int getNumberOfConnections() {
    return this.handlers.size();
  }

  public int getLocalPort() {
    if (this.serverSocket != null) {
      return this.serverSocket.getLocalPort();
    }
    return -1;
  }

  public ServerStatus getServerStatus() {
    synchronized (this.status) {
      return this.status;
    }
  }

  @Override
  public String toString() {
    return "ProxyServer[" + getName() + "]";
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

  /** Possible states of a {@link Server}. */
  public enum ServerStatus {
    NEW,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED
  }
}
