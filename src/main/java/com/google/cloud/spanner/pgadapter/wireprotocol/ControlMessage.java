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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.api.core.InternalApi;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.metadata.SendResultSetState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.PartitionQueryResult;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import com.google.cloud.spanner.pgadapter.wireoutput.CommandCompleteResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.EmptyQueryResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ErrorResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.PortalSuspendedResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.WireOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Generic representation for a control wire message: that is, a message which does not handle any
 * form of start-up, but reather general communications.
 */
@InternalApi
public abstract class ControlMessage extends WireMessage {
  private static final Logger logger = Logger.getLogger(ControlMessage.class.getName());

  /** Maximum number of invalid messages in a row allowed before we terminate the connection. */
  static final int MAX_INVALID_MESSAGE_COUNT = 50;

  /**
   * Token that is used to mark {@link ControlMessage}s that are manually created to execute a
   * {@link QueryMessage}.
   */
  public enum ManuallyCreatedToken {
    MANUALLY_CREATED_TOKEN
  }

  private final ManuallyCreatedToken manuallyCreatedToken;

  public ControlMessage(ConnectionHandler connection) throws IOException {
    super(connection, connection.getConnectionMetadata().getInputStream().readInt());
    this.manuallyCreatedToken = null;
  }

  /** Constructor for manually created Control messages. */
  protected ControlMessage(ConnectionHandler connection, int length, ManuallyCreatedToken token) {
    super(connection, length);
    this.manuallyCreatedToken = token;
  }

  public boolean isExtendedProtocol() {
    return manuallyCreatedToken == null;
  }

  /**
   * Factory method to create the message from the specific command type char.
   *
   * @param connection The connection handler object setup with the ability to send/receive.
   * @return The constructed wire message given the input message.
   * @throws Exception If construction or reading fails.
   */
  public static ControlMessage create(ConnectionHandler connection) throws Exception {
    boolean validMessage = true;
    char nextMsg = (char) connection.getConnectionMetadata().getInputStream().readUnsignedByte();
    try {
      if (connection.getStatus() == ConnectionStatus.COPY_IN) {
        switch (nextMsg) {
          case CopyDoneMessage.IDENTIFIER:
            return new CopyDoneMessage(connection);
          case CopyDataMessage.IDENTIFIER:
            return new CopyDataMessage(connection);
          case CopyFailMessage.IDENTIFIER:
            return new CopyFailMessage(connection);
          case SyncMessage.IDENTIFIER:
          case FlushMessage.IDENTIFIER:
            // Skip sync/flush in COPY_IN. This is consistent with real PostgreSQL which also does
            // this to accommodate clients that do not check what type of statement they sent in an
            // ExecuteMessage, and instead always blindly send a flush/sync after each execute.
            return SkipMessage.createForValidStream(connection);
          default:
            // Skip other unexpected messages and throw an exception to fail the copy operation.
            validMessage = false;
            SkipMessage.createForInvalidStream(connection);
            throw new IllegalStateException(
                String.format(
                    "Expected CopyData ('d'), CopyDone ('c') or CopyFail ('f') messages, got: '%c'",
                    nextMsg));
        }
      } else {
        switch (nextMsg) {
          case QueryMessage.IDENTIFIER:
            return new QueryMessage(connection);
          case ParseMessage.IDENTIFIER:
            return new ParseMessage(connection);
          case BindMessage.IDENTIFIER:
            return new BindMessage(connection);
          case DescribeMessage.IDENTIFIER:
            return new DescribeMessage(connection);
          case ExecuteMessage.IDENTIFIER:
            return new ExecuteMessage(connection);
          case CloseMessage.IDENTIFIER:
            return new CloseMessage(connection);
          case TerminateMessage.IDENTIFIER:
            return new TerminateMessage(connection);
          case FunctionCallMessage.IDENTIFIER:
            return new FunctionCallMessage(connection);
          case FlushMessage.IDENTIFIER:
            return new FlushMessage(connection);
          case SyncMessage.IDENTIFIER:
            return new SyncMessage(connection);
          case CopyDoneMessage.IDENTIFIER:
          case CopyDataMessage.IDENTIFIER:
          case CopyFailMessage.IDENTIFIER:
            // Silently skip COPY messages in non-COPY mode. This is consistent with the PG wire
            // protocol. If we continue to receive COPY messages while in non-COPY mode, we'll
            // terminate the connection to prevent the server from being flooded with invalid
            // messages.
            validMessage = false;
            // Note: The stream itself is still valid as we received a message that we recognized.
            return SkipMessage.createForValidStream(connection);
          default:
            throw new IllegalStateException(String.format("Unknown message: %c", nextMsg));
        }
      }
    } finally {
      if (validMessage) {
        connection.clearInvalidMessageCount();
      } else {
        connection.increaseInvalidMessageCount();
        if (connection.getInvalidMessageCount() > MAX_INVALID_MESSAGE_COUNT) {
          new ErrorResponse(
                  connection.getConnectionMetadata().getOutputStream(),
                  PGException.newBuilder(
                          String.format(
                              "Received %d invalid/unexpected messages. Last received message: '%c'",
                              connection.getInvalidMessageCount(), nextMsg))
                      .setSQLState(SQLState.ProtocolViolation)
                      .setSeverity(Severity.FATAL)
                      .build())
              .send();
          connection.setStatus(ConnectionStatus.TERMINATED);
        }
      }
    }
  }

  /**
   * Extract format codes from message (useful for both input and output format codes).
   *
   * @param input The data stream containing the user request.
   * @return A list of format codes.
   * @throws Exception If reading fails in any way.
   */
  protected static List<Short> getFormatCodes(DataInputStream input) throws Exception {
    List<Short> formatCodes = new ArrayList<>();
    short numberOfFormatCodes = input.readShort();
    for (int i = 0; i < numberOfFormatCodes; i++) {
      formatCodes.add(input.readShort());
    }
    return formatCodes;
  }

  public enum PreparedType {
    Portal,
    Statement;

    static PreparedType prepareType(char type) {
      switch (type) {
        case ('P'):
          return PreparedType.Portal;
        case ('S'):
          return PreparedType.Statement;
        default:
          throw new IllegalArgumentException("Unknown Statement type!");
      }
    }
  }

  /**
   * Takes an Exception Object and relates its results to the user within the client.
   *
   * @param exception The exception to be related.
   * @throws Exception if there is some issue in the sending of the error messages.
   */
  protected void handleError(Exception exception) throws Exception {
    new ErrorResponse(this.outputStream, PGExceptionFactory.toPGException(exception)).send(false);
  }

  /**
   * Sends the result of an execute or query to the client. The type of message depends on the type
   * of result of the statement. This method may also be called multiple times for one result if the
   * client has set a max number of rows to fetch for each execute message. The {@link
   * IntermediateStatement} will cache the result in between calls and continue serving rows from
   * the position it was left off after the last execute message.
   *
   * <p>NOTE: This method does not flush the output stream.
   */
  public void sendSpannerResult(IntermediateStatement statement, QueryMode mode, long maxRows)
      throws Exception {
    String command = statement.getCommandTag();
    if (Strings.isNullOrEmpty(command)) {
      new EmptyQueryResponse(this.outputStream).send(false);
      return;
    }
    if (statement.getStatementResult() == null) {
      return;
    }

    switch (statement.getStatementType()) {
      case DDL:
      case CLIENT_SIDE:
      case UNKNOWN:
        new CommandCompleteResponse(this.outputStream, command).send(false);
        break;
      case QUERY:
        SendResultSetState state = sendResultSet(statement, mode, maxRows);
        statement.setHasMoreData(state.hasMoreRows());
        if (state.hasMoreRows()) {
          new PortalSuspendedResponse(this.outputStream).send(false);
        } else {
          statement.close();
          new CommandCompleteResponse(this.outputStream, state.getCommandAndNumRows()).send(false);
        }
        break;
      case UPDATE:
        // For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows
        // inserted. oid used to be the object ID of the inserted row if rows was 1 and the target
        // table had OIDs, but OIDs system columns are not supported anymore; therefore oid is
        // always 0.
        command += ("INSERT".equals(command) ? " 0 " : " ") + statement.getUpdateCount();
        new CommandCompleteResponse(this.outputStream, command).send(false);
        break;
      default:
        throw new IllegalStateException("Unknown statement type: " + statement.getStatement());
    }
  }

  /**
   * Simple Adapter, which takes specific results from Spanner, and packages them in a format
   * Postgres understands.
   *
   * @param describedResult Statement output by Spanner.
   * @param mode Specific Query Mode required for this specific message for Postgres
   * @param maxRows Maximum number of rows requested
   * @return An adapted representation with specific metadata which PG wire requires.
   * @throws com.google.cloud.spanner.SpannerException if traversing the {@link ResultSet} fails.
   */
  public SendResultSetState sendResultSet(
      IntermediateStatement describedResult, QueryMode mode, long maxRows) throws Exception {
    Preconditions.checkArgument(
        describedResult.containsResultSet(), "The statement result must be a result set");
    long rows;
    StatementResult statementResult = describedResult.getStatementResult();
    boolean hasData;
    if (statementResult instanceof PartitionQueryResult) {
      hasData = false;
      PartitionQueryResult partitionQueryResult = (PartitionQueryResult) statementResult;
      rows =
          sendPartitionedQuery(
              describedResult,
              mode,
              partitionQueryResult.getBatchTransactionId(),
              partitionQueryResult.getPartitions());
    } else {
      hasData = describedResult.isHasMoreData();
      ResultSet resultSet = describedResult.getStatementResult().getResultSet();
      SendResultSetRunnable runnable =
          SendResultSetRunnable.forResultSet(
              describedResult, resultSet, maxRows, mode, SettableFuture.create(), hasData);
      rows = runnable.call();
      hasData = runnable.hasData;
    }

    for (WireOutput suffix : describedResult.createResultSuffix()) {
      suffix.send(false);
    }
    return new SendResultSetState(describedResult.getCommandTag(), rows, hasData);
  }

  long sendPartitionedQuery(
      IntermediateStatement describedResult,
      QueryMode mode,
      BatchTransactionId batchTransactionId,
      List<Partition> partitions) {
    ListeningExecutorService executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                Math.min(8 * Runtime.getRuntime().availableProcessors(), partitions.size())));
    List<ListenableFuture<Long>> futures = new ArrayList<>(partitions.size());
    SettableFuture<Boolean> prefixSent = SettableFuture.create();
    Connection spannerConnection = connection.getSpannerConnection();
    Spanner spanner = ConnectionOptionsHelper.getSpanner(spannerConnection);
    BatchClient batchClient = spanner.getBatchClient(connection.getDatabaseId());
    BatchReadOnlyTransaction batchReadOnlyTransaction =
        batchClient.batchReadOnlyTransaction(batchTransactionId);
    Context context =
        Context.current()
            .withValue(
                SpannerOptions.CALL_CONTEXT_CONFIGURATOR_KEY,
                new SpannerOptions.CallContextConfigurator() {
                  @Override
                  public <ReqT, RespT> ApiCallContext configure(
                      ApiCallContext context, ReqT request, MethodDescriptor<ReqT, RespT> method) {
                    return GrpcCallContext.createDefault().withTimeout(Duration.ofHours(24L));
                  }
                });
    for (int i = 0; i < partitions.size(); i++) {
      futures.add(
          executorService.submit(
              context.wrap(
                  SendResultSetRunnable.forPartition(
                      describedResult,
                      batchReadOnlyTransaction,
                      partitions.get(i),
                      mode,
                      i == 0,
                      prefixSent))));
    }
    executorService.shutdown();
    try {
      @SuppressWarnings("UnstableApiUsage")
      List<Long> rowCounts = Futures.allAsList(futures).get();
      long rowCount = rowCounts.stream().reduce(Long::sum).orElse(0L);
      logger.log(Level.INFO, String.format("Sent %d rows from partitioned query", rowCount));
      return rowCount;
    } catch (ExecutionException executionException) {
      logger.log(
          Level.WARNING, "Sending partitioned query result failed", executionException.getCause());
      executorService.shutdownNow();
      throw SpannerExceptionFactory.asSpannerException(executionException.getCause());
    } catch (InterruptedException interruptedException) {
      logger.log(
          Level.WARNING, "Sending partitioned query result interrupted", interruptedException);
      executorService.shutdownNow();
      throw SpannerExceptionFactory.propagateInterrupt(interruptedException);
    } finally {
      batchReadOnlyTransaction.cleanup();
    }
  }

  static final class SendResultSetRunnable implements Callable<Long> {
    private final IntermediateStatement describedResult;
    private ResultSet resultSet;
    private Converter converter;
    private final BatchReadOnlyTransaction batchReadOnlyTransaction;
    private final Partition partition;
    private final long maxRows;
    private final QueryMode mode;
    private final boolean includePrefix;
    private final SettableFuture<Boolean> prefixSent;
    private boolean hasData;

    static SendResultSetRunnable forResultSet(
        IntermediateStatement describedResult,
        ResultSet resultSet,
        long maxRows,
        QueryMode mode,
        SettableFuture<Boolean> prefixSent,
        boolean hasData) {
      return new SendResultSetRunnable(
          describedResult, resultSet, maxRows, mode, true, prefixSent, hasData);
    }

    static SendResultSetRunnable forPartition(
        IntermediateStatement describedResult,
        BatchReadOnlyTransaction batchReadOnlyTransaction,
        Partition partition,
        QueryMode mode,
        boolean includePrefix,
        SettableFuture<Boolean> prefixSent) {
      return new SendResultSetRunnable(
          describedResult, batchReadOnlyTransaction, partition, mode, includePrefix, prefixSent);
    }

    private SendResultSetRunnable(
        IntermediateStatement describedResult,
        ResultSet resultSet,
        long maxRows,
        QueryMode mode,
        boolean includePrefix,
        SettableFuture<Boolean> prefixSent,
        boolean hasData) {
      this.describedResult = describedResult;
      this.resultSet = resultSet;
      this.converter =
          new Converter(
              describedResult,
              mode,
              describedResult.getConnectionHandler().getServer().getOptions(),
              resultSet);
      this.batchReadOnlyTransaction = null;
      this.partition = null;
      this.maxRows = maxRows;
      this.mode = mode;
      this.includePrefix = includePrefix;
      this.prefixSent = prefixSent;
      this.hasData = hasData;
    }

    private SendResultSetRunnable(
        IntermediateStatement describedResult,
        BatchReadOnlyTransaction batchReadOnlyTransaction,
        Partition partition,
        QueryMode mode,
        boolean includePrefix,
        SettableFuture<Boolean> prefixSent) {
      this.describedResult = describedResult;
      this.resultSet = null;
      this.batchReadOnlyTransaction = batchReadOnlyTransaction;
      this.partition = partition;
      this.maxRows = 0L;
      this.mode = mode;
      this.includePrefix = includePrefix;
      this.prefixSent = prefixSent;
      this.hasData = false;
    }

    @Override
    public Long call() throws Exception {
      try {
        if (resultSet == null && batchReadOnlyTransaction != null && partition != null) {
          // Note: It is OK not to close this result set, as the underlying transaction and session
          // will be cleaned up at a later moment.
          try {
            resultSet = batchReadOnlyTransaction.execute(partition);
            converter =
                new Converter(
                    describedResult,
                    mode,
                    describedResult.getConnectionHandler().getServer().getOptions(),
                    resultSet);
            hasData = resultSet.next();
          } catch (Throwable t) {
            if (includePrefix) {
              synchronized (describedResult) {
                prefixSent.setException(t);
              }
            }
            throw t;
          }
        }
        if (includePrefix) {
          try {
            for (WireOutput prefix : describedResult.createResultPrefix(resultSet)) {
              prefix.send(false);
            }
            prefixSent.set(true);
          } catch (Throwable t) {
            prefixSent.setException(t);
            throw t;
          }
        }
        // Wait until the prefix (if any) has been sent.
        prefixSent.get();
        long rows = 0L;
        while (hasData) {
          if (Thread.interrupted()) {
            throw PGExceptionFactory.newQueryCancelledException();
          }
          WireOutput wireOutput = describedResult.createDataRowResponse(converter);
          synchronized (describedResult) {
            wireOutput.send(false);
          }
          rows++;
          hasData = resultSet.next();
          if (rows % 1000 == 0) {
            logger.log(Level.INFO, String.format("Sent %d rows", rows));
          }
          if (rows == maxRows) {
            break;
          }
        }
        return rows;
      } finally {
        if (converter != null) {
          converter.close();
        }
      }
    }
  }
}
