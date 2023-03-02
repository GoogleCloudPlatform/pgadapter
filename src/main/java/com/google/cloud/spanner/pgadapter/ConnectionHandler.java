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
import com.google.auth.Credentials;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerException.ResourceNotFoundException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.DescribeResult;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.ExtendedQueryProtocolHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePortalStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediatePreparedStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.TerminateResponse;
import com.google.cloud.spanner.pgadapter.wireprotocol.BootstrapMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.cloud.spanner.pgadapter.wireprotocol.WireMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.InstanceName;
import com.google.spanner.v1.DatabaseName;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSocketFactory;

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
  private Socket socket;
  private final Map<String, IntermediatePreparedStatement> statementsMap = new HashMap<>();
  private final Cache<String, Future<DescribeResult>> autoDescribedStatementsCache =
      CacheBuilder.newBuilder()
          .expireAfterWrite(Duration.ofMinutes(30L))
          .maximumSize(5000L)
          .concurrencyLevel(1)
          .build();
  private final Map<String, IntermediatePortalStatement> portalsMap = new HashMap<>();
  private static final Map<Integer, ConnectionHandler> CONNECTION_HANDLERS =
      new ConcurrentHashMap<>();
  private volatile ConnectionStatus status = ConnectionStatus.UNAUTHENTICATED;
  private final int connectionId;
  private final int secret;
  // Separate the following from the threat ID generator, since PG connection IDs are maximum
  //  32 bytes, and shouldn't be incremented on failed startups.
  private static final AtomicInteger incrementingConnectionId = new AtomicInteger(0);
  private ConnectionMetadata connectionMetadata;
  private WireMessage message;
  private int invalidMessagesCount;
  private Connection spannerConnection;
  private DatabaseId databaseId;
  private WellKnownClient wellKnownClient = WellKnownClient.UNSPECIFIED;
  private boolean hasDeterminedClientUsingQuery;
  private ExtendedQueryProtocolHandler extendedQueryProtocolHandler;
  private CopyStatement activeCopyStatement;

  ConnectionHandler(ProxyServer server, Socket socket) {
    this(server, socket, null);
  }

  /** Constructor only for testing. */
  @VisibleForTesting
  ConnectionHandler(ProxyServer server, Socket socket, Connection spannerConnection) {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.secret = new SecureRandom().nextInt();
    this.connectionId = incrementingConnectionId.incrementAndGet();
    CONNECTION_HANDLERS.put(this.connectionId, this);
    setDaemon(true);
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s created for client %s",
                getName(), socket.getInetAddress().getHostAddress()));
    this.spannerConnection = spannerConnection;
  }

  void createSSLSocket() throws IOException {
    this.socket =
        ((SSLSocketFactory) SSLSocketFactory.getDefault()).createSocket(socket, null, true);
  }

  @InternalApi
  public void connectToSpanner(String database, @Nullable Credentials credentials) {
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
    ConnectionOptions.Builder connectionOptionsBuilder = ConnectionOptions.newBuilder().setUri(uri);
    if (credentials != null) {
      connectionOptionsBuilder =
          ConnectionOptionsHelper.setCredentials(connectionOptionsBuilder, credentials);
    }
    ConnectionOptions connectionOptions = connectionOptionsBuilder.build();
    Connection spannerConnection = connectionOptions.getConnection();
    try {
      // Note: Calling getDialect() will cause a SpannerException if the connection itself is
      // invalid, for example as a result of the credentials being wrong.
      if (spannerConnection.getDialect() != Dialect.POSTGRESQL) {
        spannerConnection.close();
        throw PGException.newBuilder(
                String.format(
                    "The database uses dialect %s. Currently PGAdapter only supports connections to PostgreSQL dialect databases. "
                        + "These can be created using https://cloud.google.com/spanner/docs/quickstart-console#postgresql",
                    spannerConnection.getDialect()))
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.SQLServerRejectedEstablishmentOfSQLConnection)
            .build();
      }
    } catch (InstanceNotFoundException | DatabaseNotFoundException notFoundException) {
      SpannerException exceptionToThrow = notFoundException;
      //noinspection finally
      try {
        // Include more information about the available databases if someone tried to connect using
        // psql.
        if (getWellKnownClient() == WellKnownClient.PSQL) {
          Spanner spanner = ConnectionOptionsHelper.getSpanner(spannerConnection);
          String availableDatabases =
              listDatabasesOrInstances(
                  notFoundException, getServer().getOptions().getDatabaseName(database), spanner);
          exceptionToThrow =
              SpannerExceptionFactory.newSpannerException(
                  notFoundException.getErrorCode(),
                  notFoundException.getMessage() + "\n" + availableDatabases);
        }
      } finally {
        spannerConnection.close();
        throw exceptionToThrow;
      }
    } catch (SpannerException e) {
      spannerConnection.close();
      throw e;
    }
    this.spannerConnection = spannerConnection;
    this.databaseId = connectionOptions.getDatabaseId();
    this.extendedQueryProtocolHandler = new ExtendedQueryProtocolHandler(this);
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
    if (runConnection(false) == RunConnectionState.RESTART_WITH_SSL) {
      logger.log(
          Level.INFO,
          () ->
              String.format(
                  "Connection handler with ID %s is restarted so it can use SSL", getName()));
      restartConnectionWithSsl();
    }
  }

  void restartConnectionWithSsl() {
    try {
      createSSLSocket();
      runConnection(true);
    } catch (IOException ioException) {
      PGException pgException =
          PGException.newBuilder(
                  "Failed to create SSL socket: "
                      + (ioException.getMessage() == null
                          ? ioException.getClass().getName()
                          : ioException.getMessage()))
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.InternalError)
              .build();
      try {
        handleError(pgException);
      } catch (Exception ignore) {
      }
      throw pgException;
    }
  }

  enum RunConnectionState {
    RESTART_WITH_SSL,
    TERMINATED
  }

  /**
   * Starts listening for incoming messages on the network socket. Returns RESTART_WITH_SSL if the
   * listening process should be restarted with SSL.
   */
  private RunConnectionState runConnection(boolean ssl) {
    RunConnectionState result = RunConnectionState.TERMINATED;
    try (ConnectionMetadata connectionMetadata =
        new ConnectionMetadata(this.socket.getInputStream(), this.socket.getOutputStream())) {
      this.connectionMetadata = connectionMetadata;

      try {
        this.message = this.server.recordMessage(BootstrapMessage.create(this));
        if (!ssl
            && getServer().getOptions().getSslMode().isSslEnabled()
            && this.message instanceof SSLMessage) {
          this.message.send();
          this.connectionMetadata.markForRestart();
          result = RunConnectionState.RESTART_WITH_SSL;
          return result;
        }
        // Check whether the connection is valid. That is, the connection satisfies any restrictions
        // on non-localhost connections and SSL requirements.
        if (!checkValidConnection(ssl)) {
          return result;
        }
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
          handleMessages();
        }
      } catch (PGException pgException) {
        this.handleError(pgException);
      } catch (Exception exception) {
        this.handleError(
            PGException.newBuilder(exception)
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
      if (result != RunConnectionState.RESTART_WITH_SSL) {
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
                  String.format(
                      "Exception while closing connection handler with ID %s", getName()));
        }
        this.server.deregister(this);
        logger.log(
            Level.INFO, () -> String.format("Connection handler with ID %s closed", getName()));
      }
    }
    return result;
  }

  boolean checkValidConnection(boolean ssl) throws Exception {
    // Allow SSL connections from non-localhost even if the localhost check has not explicitly
    // been disabled.
    if (!ssl
        && !server.getOptions().disableLocalhostCheck()
        && !this.socket.getInetAddress().isAnyLocalAddress()
        && !this.socket.getInetAddress().isLoopbackAddress()) {
      handleError(
          PGException.newBuilder("This proxy may only be accessed from localhost.")
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.SQLServerRejectedEstablishmentOfSQLConnection)
              .build());
      return false;
    }
    if (!ssl && server.getOptions().getSslMode() == SslMode.Require) {
      handleError(
          PGException.newBuilder("This proxy requires SSL.")
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.SQLServerRejectedEstablishmentOfSQLConnection)
              .build());
      return false;
    }
    return true;
  }

  /**
   * Reads and handles wire-protocol messages. This method is normally only called from this {@link
   * ConnectionHandler}, but certain sub-protocols such as the COPY protocol also need to process
   * messages in line.
   */
  public void handleMessages() throws Exception {
    try {
      message.nextHandler();
      message.send();
    } catch (IllegalArgumentException | IllegalStateException | EOFException fatalException) {
      this.handleError(
          PGException.newBuilder(fatalException)
              .setSeverity(Severity.FATAL)
              .setSQLState(SQLState.InternalError)
              .build());
      // Only terminate the connection if we are not in COPY_IN mode. In COPY_IN mode the mode will
      // switch to normal mode in these cases.
      if (this.status != ConnectionStatus.COPY_IN) {
        terminate();
      }
    } catch (Exception exception) {
      this.handleError(PGExceptionFactory.toPGException(exception));
    }
  }

  /** Called when a Terminate message is received. This closes this {@link ConnectionHandler}. */
  public void handleTerminate() {
    synchronized (this) {
      closeAllPortals();
      if (this.spannerConnection != null) {
        this.spannerConnection.close();
      }
      this.status = ConnectionStatus.TERMINATED;
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
  void handleError(PGException exception) throws Exception {
    logger.log(
        Level.WARNING,
        exception,
        () ->
            String.format("Exception on connection handler with ID %s: %s", getName(), exception));
    DataOutputStream output = getConnectionMetadata().getOutputStream();
    if (this.status == ConnectionStatus.TERMINATED
        || this.status == ConnectionStatus.UNAUTHENTICATED) {
      new ErrorResponse(this, exception).send();
      new TerminateResponse(output).send();
    } else if (this.status == ConnectionStatus.COPY_IN) {
      new ErrorResponse(this, exception).send();
    } else {
      new ErrorResponse(this, exception).send();
      new ReadyResponse(output, ReadyResponse.Status.IDLE).send();
    }
  }

  /** Closes portals and statements if the result of an execute was the end of a transaction. */
  public void cleanUp(IntermediateStatement statement) throws Exception {
    if (!statement.isHasMoreData() && statement.isBound()) {
      statement.close();
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

  public void closePortal(String portalName) throws Exception {
    if (!hasPortal(portalName)) {
      throw new IllegalStateException("Unregistered portal: " + portalName);
    }
    IntermediatePortalStatement portal = this.portalsMap.get(portalName);
    portal.close();
  }

  public boolean hasPortal(String portalName) {
    return this.portalsMap.containsKey(portalName);
  }

  /**
   * To be used by a cancellation command to cancel a currently running statement, as contained in a
   * specific connection identified by connectionId. Since cancellation is a flimsy contract at
   * best, it is not imperative that the cancellation run, but it should be attempted nonetheless.
   *
   * @param connectionId The connection whose statement must be cancelled.
   * @param secret The secret value linked to the connection that is being cancelled. If it does not
   *     match, we cannot cancel.
   * @return true if the statement was cancelled.
   */
  public boolean cancelActiveStatement(int connectionId, int secret) {
    if (connectionId == this.connectionId) {
      // You can't cancel your own statement.
      return false;
    }
    ConnectionHandler connectionToCancel = CONNECTION_HANDLERS.get(connectionId);
    if (connectionToCancel == null) {
      logger.log(
          Level.WARNING,
          () ->
              MessageFormat.format(
                  "User attempted to cancel an unknown connection. Connection: {0}", connectionId));
      return false;
    }
    if (secret != connectionToCancel.secret) {
      logger.log(
          Level.WARNING,
          () ->
              MessageFormat.format(
                  "User attempted to cancel a connection with the incorrect secret."
                      + "Connection: {0}, Secret: {1}, Expected Secret: {2}",
                  connectionId, secret, connectionToCancel.secret));
      // Since the user does not accept a response, there is no need to except here: simply return.
      return false;
    }
    // We can mostly ignore the exception since cancel does not expect any result (positive or
    // otherwise)
    try {
      connectionToCancel.getSpannerConnection().cancel();
      connectionToCancel.interrupt();
      return true;
    } catch (Throwable ignore) {
    }
    return false;
  }

  public IntermediatePreparedStatement getStatement(String statementName) {
    if (!hasStatement(statementName)) {
      throw PGExceptionFactory.newPGException(
          "prepared statement " + statementName + " does not exist");
    }
    return this.statementsMap.get(statementName);
  }

  public void registerStatement(String statementName, IntermediatePreparedStatement statement) {
    this.statementsMap.put(statementName, statement);
  }

  /**
   * Returns the parameter types of a cached auto-described statement, or null if none is available
   * in the cache.
   */
  public Future<DescribeResult> getAutoDescribedStatement(String sql) {
    return this.autoDescribedStatementsCache.getIfPresent(sql);
  }

  /** Stores the parameter types of an auto-described statement in the cache. */
  public void registerAutoDescribedStatement(String sql, Future<DescribeResult> describeResult) {
    this.autoDescribedStatementsCache.put(sql, describeResult);
  }

  public void closeStatement(String statementName) {
    if (!hasStatement(statementName)) {
      throw PGExceptionFactory.newPGException(
          "prepared statement " + statementName + " does not exist");
    }
    this.statementsMap.remove(statementName);
  }

  public void closeAllStatements() {
    Set<String> names = new HashSet<>(this.statementsMap.keySet());
    for (String statementName : names) {
      closeStatement(statementName);
    }
  }

  public void setActiveCopyStatement(CopyStatement copyStatement) {
    this.activeCopyStatement = copyStatement;
  }

  public CopyStatement getActiveCopyStatement() {
    return this.activeCopyStatement;
  }

  public void clearActiveCopyStatement() {
    this.activeCopyStatement = null;
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

  public DatabaseId getDatabaseId() {
    return this.databaseId;
  }

  public int getConnectionId() {
    return this.connectionId;
  }

  public int getSecret() {
    return this.secret;
  }

  public void setMessageState(WireMessage message) {
    this.message = this.server.recordMessage(message);
  }

  /**
   * Returns the number of invalid messages that this connection has received in a row. This can for
   * example happen if a client has initiated a COPY operation and the copy operation fails on the
   * server. The server will then respond with an error response, but if the client fails to read
   * that message and continues to send copy data messages, the server could get flooded. This value
   * is used to detect such a situation and breaks the connection if too many invalid messages in a
   * row are received.
   */
  public int getInvalidMessageCount() {
    return this.invalidMessagesCount;
  }

  /** Increases the number of invalid messages that was received in a row by 1. */
  public void increaseInvalidMessageCount() {
    this.invalidMessagesCount++;
  }

  /**
   * Clears the number of invalid messages that was received. This is called whenever a valid
   * message is encountered.
   */
  public void clearInvalidMessageCount() {
    this.invalidMessagesCount = 0;
  }

  public ConnectionMetadata getConnectionMetadata() {
    return connectionMetadata;
  }

  public ExtendedQueryProtocolHandler getExtendedQueryProtocolHandler() {
    return extendedQueryProtocolHandler;
  }

  public synchronized ConnectionStatus getStatus() {
    return status;
  }

  public synchronized void setStatus(ConnectionStatus status) {
    this.status = status;
  }

  public WellKnownClient getWellKnownClient() {
    return wellKnownClient;
  }

  public void setWellKnownClient(WellKnownClient wellKnownClient) {
    this.wellKnownClient = wellKnownClient;
    if (this.wellKnownClient != WellKnownClient.UNSPECIFIED) {
      logger.log(
          Level.INFO,
          () ->
              String.format(
                  "Well-known client %s detected for connection %d.",
                  this.wellKnownClient, getConnectionId()));
    }
  }

  /**
   * This is called by the simple {@link
   * com.google.cloud.spanner.pgadapter.wireprotocol.QueryMessage} to give the connection the
   * opportunity to determine the client that is connected based on the SQL string that is being
   * executed.
   */
  public void maybeDetermineWellKnownClient(Statement statement) {
    if (!this.hasDeterminedClientUsingQuery) {
      if (this.wellKnownClient == WellKnownClient.UNSPECIFIED
          && getServer().getOptions().shouldAutoDetectClient()) {
        setWellKnownClient(ClientAutoDetector.detectClient(ImmutableList.of(statement)));
      }
      maybeSetApplicationName();
    }
    // Make sure that we only try to detect the client once.
    this.hasDeterminedClientUsingQuery = true;
  }

  /**
   * This is called by the extended query protocol {@link
   * com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage} to give the connection the
   * opportunity to determine the client that is connected based on the data in the (first) parse
   * messages.
   */
  public void maybeDetermineWellKnownClient(ParseMessage parseMessage) {
    if (!this.hasDeterminedClientUsingQuery) {
      if (this.wellKnownClient == WellKnownClient.UNSPECIFIED
          && getServer().getOptions().shouldAutoDetectClient()) {
        setWellKnownClient(ClientAutoDetector.detectClient(parseMessage));
      }
      maybeSetApplicationName();
    }
    // Make sure that we only try to detect the client once.
    this.hasDeterminedClientUsingQuery = true;
  }

  private void maybeSetApplicationName() {
    try {
      if (this.wellKnownClient != WellKnownClient.UNSPECIFIED
          && getExtendedQueryProtocolHandler() != null
          && Strings.isNullOrEmpty(
              getExtendedQueryProtocolHandler()
                  .getBackendConnection()
                  .getSessionState()
                  .get(null, "application_name")
                  .getSetting())) {
        getExtendedQueryProtocolHandler()
            .getBackendConnection()
            .getSessionState()
            .set(null, "application_name", wellKnownClient.name().toLowerCase(Locale.ENGLISH));
        getExtendedQueryProtocolHandler().getBackendConnection().getSessionState().commit();
      }
    } catch (Throwable ignore) {
      // Safeguard against a theoretical situation that 'application_name' has been removed from
      // the list of settings. Just ignore this situation, as the only consequence is that the
      // 'application_name' setting has not been set.
    }
  }

  /** Status of a {@link ConnectionHandler} */
  public enum ConnectionStatus {
    UNAUTHENTICATED,
    AUTHENTICATED,
    COPY_IN,
    COPY_DONE,
    COPY_FAILED,
    TERMINATED,
  }

  /**
   * PostgreSQL query mode (see also <a
   * href="https://www.postgresql.org/docs/current/protocol-flow.html">here</a>).
   */
  public enum QueryMode {
    SIMPLE,
    EXTENDED
  }

  static String listDatabasesOrInstances(
      ResourceNotFoundException notFoundException, DatabaseName databaseName, Spanner spanner) {
    StringBuilder result = new StringBuilder();
    if (notFoundException instanceof InstanceNotFoundException) {
      InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
      result
          .append("Instance ")
          .append(InstanceName.of(databaseName.getProject(), databaseName.getInstance()))
          .append(" not found.\n\n")
          .append("These instances are available in project ")
          .append(databaseName.getProject())
          .append(":\n");
      for (Instance instance : instanceAdminClient.listInstances().iterateAll()) {
        result.append("\t").append(instance.getId()).append("\n");
      }
    } else {
      DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
      result
          .append("Database ")
          .append(databaseName)
          .append(" not found.\n\n")
          .append("These PostgreSQL databases are available on instance ")
          .append(InstanceName.of(databaseName.getProject(), databaseName.getInstance()))
          .append(":\n");
      for (Database database :
          databaseAdminClient.listDatabases(databaseName.getInstance()).iterateAll()) {
        if (database.getDialect() == Dialect.POSTGRESQL) {
          result.append("\t").append(database.getId()).append("\n");
        }
      }
    }
    return result.toString();
  }
}
