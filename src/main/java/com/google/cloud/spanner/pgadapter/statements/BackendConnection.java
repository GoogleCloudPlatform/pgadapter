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

import static com.google.cloud.spanner.pgadapter.error.PGExceptionFactory.toPGException;
import static com.google.cloud.spanner.pgadapter.statements.IntermediateStatement.PARSER;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.addLimitIfParameterizedOffset;
import static com.google.cloud.spanner.pgadapter.statements.SimpleParser.replaceForUpdate;

import com.google.api.core.InternalApi;
import com.google.cloud.ByteArray;
import com.google.cloud.Tuple;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerBatchUpdateException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.AutocommitDmlMode;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.connection.ResultSetHelper;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.connection.StatementResult.ClientSideStatementType;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SessionStatementParser.SessionStatement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.utils.CopyDataReceiver;
import com.google.cloud.spanner.pgadapter.utils.MutationWriter;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse.Status;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class emulates a backend PostgreSQL connection. Statements are buffered in memory until a
 * flush/sync is received. This makes it possible to batch multiple statements together before
 * sending these to Cloud Spanner. This class also keeps track of the transaction status of the
 * connection.
 */
@InternalApi
public class BackendConnection {
  public static final String TRANSACTION_ABORTED_ERROR =
      "current transaction is aborted, commands ignored until end of transaction block";

  /**
   * Connection state indicates whether the backend connection is idle, in a transaction or in an
   * aborted transaction.
   */
  public enum ConnectionState {
    /** Connection is idle, no transaction is active. */
    IDLE(Status.IDLE),
    /** An implicit or explicit transaction is active. */
    TRANSACTION(Status.TRANSACTION),
    /** The current transaction is aborted. All statements are refused until rollback. */
    ABORTED(Status.FAILED);

    private final ReadyResponse.Status readyResponseStatus;

    ConnectionState(ReadyResponse.Status readyResponseStatus) {
      this.readyResponseStatus = readyResponseStatus;
    }

    public ReadyResponse.Status getReadyResponseStatus() {
      return this.readyResponseStatus;
    }
  }

  /**
   * {@link TransactionMode} indicates whether the current transaction on a backend connection is
   * implicit or explicit. Implicit transactions are automatically committed/rolled back when a
   * batch of statements finishes execution (i.e. when we receive a Sync message).
   */
  enum TransactionMode {
    IMPLICIT,
    EXPLICIT,
    DDL_BATCH,
  }

  static <T> PGException setAndReturn(SettableFuture<T> future, Throwable throwable) {
    PGException pgException = toPGException(throwable);
    future.setException(pgException);
    return pgException;
  }

  boolean shouldReplaceStatement(Statement statement) {
    if (!localStatements.get().isEmpty() && localStatements.get().containsKey(statement.getSql())) {
      LocalStatement localStatement = localStatements.get().get(statement.getSql());
      if (localStatement != null) {
        return localStatement.hasReplacementStatement();
      }
    }
    return false;
  }

  Tuple<Statement, ParsedStatement> replaceStatement(Statement statement) {
    LocalStatement localStatement = localStatements.get().get(statement.getSql());
    Statement replacement =
        Objects.requireNonNull(localStatement).getReplacementStatement(statement);
    return Tuple.of(replacement, Objects.requireNonNull(PARSER.parse(replacement)));
  }

  /**
   * Buffered statements are kept in memory until a flush or sync message is received. This makes it
   * possible to batch multiple statements together when sending them to Cloud Spanner.
   */
  abstract class BufferedStatement<T> {
    final ParsedStatement parsedStatement;
    final Statement statement;
    final SettableFuture<T> result;

    BufferedStatement(ParsedStatement parsedStatement, Statement statement) {
      if (shouldReplaceStatement(statement)) {
        Tuple<Statement, ParsedStatement> replacement = replaceStatement(statement);
        statement = replacement.x();
        parsedStatement = replacement.y();
      }
      this.parsedStatement = parsedStatement;
      this.statement = statement;
      this.result = SettableFuture.create();
    }

    boolean isBatchingPossible() {
      return false;
    }

    abstract boolean isUpdate();

    abstract void execute();

    void checkConnectionState() {
      // Only COMMIT or ROLLBACK is allowed if we are in an ABORTED transaction.
      if (connectionState == ConnectionState.ABORTED
          && !(isCommit(parsedStatement) || isRollback(parsedStatement))) {
        throw PGExceptionFactory.newTransactionAbortedException();
      }
    }
  }

  private final class Execute extends BufferedStatement<StatementResult> {
    private final Function<Statement, Statement> statementBinder;
    private final boolean analyze;

    Execute(
        ParsedStatement parsedStatement,
        Statement statement,
        Function<Statement, Statement> statementBinder) {
      this(parsedStatement, statement, statementBinder, false);
    }

    Execute(
        ParsedStatement parsedStatement,
        Statement statement,
        Function<Statement, Statement> statementBinder,
        boolean analyze) {
      super(parsedStatement, statement);
      this.statementBinder = statementBinder;
      this.analyze = analyze;
    }

    @Override
    boolean isBatchingPossible() {
      return !analyze;
    }

    @Override
    boolean isUpdate() {
      return this.parsedStatement.isUpdate();
    }

    @Override
    void execute() {
      Statement updatedStatement = statement;
      try {
        checkConnectionState();
        // TODO(b/235719478): If the statement is a BEGIN statement and there is a COMMIT statement
        //  at a later point in the batch, and all the statements in the transaction block are
        //  SELECT statements, then we should create a read-only transaction. Also, if a transaction
        //  block always ends with a ROLLBACK, PGAdapter should skip the entire execution of that
        //  block.
        SessionStatement sessionStatement = getSessionManagementStatement(parsedStatement);
        if (!localStatements.get().isEmpty()
            && localStatements.get().containsKey(statement.getSql())
            && localStatements.get().get(statement.getSql()) != null
            && !Objects.requireNonNull(localStatements.get().get(statement.getSql()))
                .hasReplacementStatement()) {
          LocalStatement localStatement =
              Objects.requireNonNull(localStatements.get().get(statement.getSql()));
          result.set(localStatement.execute(BackendConnection.this));
        } else if (sessionStatement != null) {
          result.set(sessionStatement.execute(sessionState));
        } else if (connectionState == ConnectionState.ABORTED
            && !spannerConnection.isInTransaction()
            && (isRollback(parsedStatement) || isCommit(parsedStatement))) {
          result.set(ROLLBACK_RESULT);
        } else if (isTransactionStatement(parsedStatement) && sessionState.isForceAutocommit()) {
          result.set(NO_RESULT);
        } else if (isBegin(parsedStatement) && spannerConnection.isInTransaction()) {
          // Ignore the statement as it is a no-op to execute BEGIN when we are already in a
          // transaction. TODO: Return a warning.
          result.set(NO_RESULT);
        } else if ((isCommit(parsedStatement) || isRollback(parsedStatement))
            && !spannerConnection.isInTransaction()) {
          // Check if we are in a DDL batch that was created from an explicit transaction.
          if (transactionMode == TransactionMode.DDL_BATCH) {
            try {
              if (isCommit(parsedStatement)) {
                spannerConnection.runBatch();
              } else {
                spannerConnection.abortBatch();
              }
            } finally {
              transactionMode = TransactionMode.IMPLICIT;
            }
          }
          // Ignore the statement as it is a no-op to execute COMMIT/ROLLBACK when we are not in a
          // transaction. TODO: Return a warning.
          result.set(NO_RESULT);
        } else if (statement.getSql().isEmpty()) {
          result.set(NO_RESULT);
        } else if (parsedStatement.isDdl()) {
          if (analyze) {
            result.set(NO_RESULT);
          } else {
            result.set(ddlExecutor.execute(parsedStatement, statement));
          }
        } else {
          String sqlLowerCase = updatedStatement.getSql().toLowerCase(Locale.ENGLISH);
          // Potentially replace pg_catalog table references with common table expressions.
          updatedStatement =
              parsedStatement.getType() != StatementType.CLIENT_SIDE
                      && sessionState.isReplacePgCatalogTables()
                  ? pgCatalog.get().replacePgCatalogTables(updatedStatement, sqlLowerCase)
                  : updatedStatement;
          // TODO: Remove the check for isDelayBeginTransactionStartUntilFirstWrite when that
          //       feature is able to detect the LOCK_SCANNED_RANGES=exclusive hint as a write.
          if (sessionState.isReplaceForUpdateClause()
              && !spannerConnection.isDelayTransactionStartUntilFirstWrite()) {
            updatedStatement = replaceForUpdate(updatedStatement, sqlLowerCase);
          }
          updatedStatement = bindStatement(updatedStatement, sqlLowerCase);
          result.set(analyzeOrExecute(updatedStatement));
        }
      } catch (SpannerException spannerException) {
        // Executing queries against the information schema in a transaction is unsupported.
        // This ensures that those queries are retried using a separate single-use transaction.
        if (isUnsupportedConcurrencyModeException(spannerException)) {
          try {
            result.set(
                new QueryResult(
                    ResultSetHelper.toDirectExecuteResultSet(
                        spannerConnection
                            .getDatabaseClient()
                            .singleUse()
                            .executeQuery(updatedStatement))));
            return;
          } catch (Exception exception) {
            throw setAndReturn(result, exception);
          }
        }
        if (spannerException.getErrorCode() == ErrorCode.CANCELLED || Thread.interrupted()) {
          throw setAndReturn(result, PGExceptionFactory.newQueryCancelledException());
        } else {
          throw setAndReturn(result, spannerException);
        }
      } catch (Throwable exception) {
        throw setAndReturn(result, exception);
      }
    }

    Statement bindStatement(Statement statement, @Nullable String lowerCaseSql) {
      Statement boundStatement = statementBinder.apply(statement);
      // Add a LIMIT clause to the statement if it contains an OFFSET clause that uses a query
      // parameter and there is no existing LIMIT clause in the query.
      if (lowerCaseSql != null && sessionState.isAutoAddLimitClause()) {
        boundStatement = addLimitIfParameterizedOffset(boundStatement, lowerCaseSql);
      }
      return boundStatement;
    }

    StatementResult analyzeOrExecute(Statement statement) {
      if (analyze) {
        ResultSet resultSet;
        if (parsedStatement.isUpdate() && !parsedStatement.hasReturningClause()) {
          // TODO(#477): Single analyzeUpdate statements that are executed in an implicit
          //     transaction could use a single-use read/write transaction. Replays are not
          //     dangerous for those.

          // We handle one very specific use case here to prevent unnecessary problems: If the user
          // has started a DML batch and is then analyzing an update statement (probably a prepared
          // statement), then we use a separate transaction for that.
          if (spannerConnection.isDmlBatchActive()) {
            final Statement statementToAnalyze = statement;
            resultSet =
                spannerConnection
                    .getDatabaseClient()
                    .readWriteTransaction()
                    .run(
                        transaction -> {
                          ResultSet updateStatementMetadata =
                              transaction.analyzeUpdateStatement(
                                  statementToAnalyze, QueryAnalyzeMode.PLAN);
                          updateStatementMetadata.next();
                          return updateStatementMetadata;
                        });
          } else {
            resultSet = spannerConnection.analyzeUpdateStatement(statement, QueryAnalyzeMode.PLAN);
          }
        } else if (parsedStatement.isQuery() || parsedStatement.hasReturningClause()) {
          resultSet = spannerConnection.analyzeQuery(statement, QueryAnalyzeMode.PLAN);
        } else {
          return NO_RESULT;
        }
        return new QueryResult(resultSet);
      }
      return executeOnSpanner(statement);
    }

    StatementResult executeOnSpanner(Statement statement) {
      // Do not try to execute INSERT statements using Partitioned DML if we are in force_autocommit
      // mode. We skip this because the user has (probably) set force_autocommit for compatibility
      // reasons, so we do not want to throw an unnecessary error.
      if (sessionState.isForceAutocommit()
          && !spannerConnection.isInTransaction()
          && spannerConnection.getAutocommitDmlMode() == AutocommitDmlMode.PARTITIONED_NON_ATOMIC
          && new SimpleParser(statement.getSql()).peekKeyword("insert")) {
        try {
          spannerConnection.setAutocommitDmlMode(AutocommitDmlMode.TRANSACTIONAL);
          return spannerConnection.execute(statement);
        } finally {
          spannerConnection.setAutocommitDmlMode(AutocommitDmlMode.PARTITIONED_NON_ATOMIC);
        }
      }
      return spannerConnection.execute(statement);
    }

    /**
     * Returns true if the given exception is the error that is returned by Cloud Spanner when an
     * INFORMATION_SCHEMA query is not executed in a single-use read-only transaction.
     */
    boolean isUnsupportedConcurrencyModeException(SpannerException spannerException) {
      return spannerException.getErrorCode() == ErrorCode.INVALID_ARGUMENT
          && spannerException
              .getMessage()
              .startsWith(
                  "INVALID_ARGUMENT: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Unsupported concurrency mode in query using INFORMATION_SCHEMA.");
    }

    @Nullable
    SessionStatement getSessionManagementStatement(ParsedStatement parsedStatement) {
      if (parsedStatement.getType() == StatementType.UNKNOWN
          || (parsedStatement.getType() == StatementType.QUERY
              && parsedStatement.getSqlWithoutComments().length() >= 4
              && parsedStatement
                  .getSqlWithoutComments()
                  .substring(0, 4)
                  .equalsIgnoreCase("show"))) {
        return SessionStatementParser.parse(parsedStatement);
      }
      return null;
    }
  }

  public static final int MAX_PARTITIONS =
      Math.max(16, 2 * Runtime.getRuntime().availableProcessors());

  private final class CopyOut extends BufferedStatement<StatementResult> {

    CopyOut(ParsedStatement parsedStatement, Statement statement) {
      super(parsedStatement, statement);
    }

    @Override
    boolean isUpdate() {
      return false;
    }

    @Override
    void execute() {
      checkConnectionState();
      try {
        if (transactionMode != TransactionMode.IMPLICIT) {
          result.set(spannerConnection.execute(statement));
        } else {
          Spanner spanner = ConnectionOptionsHelper.getSpanner(spannerConnection);
          BatchClient batchClient = spanner.getBatchClient(databaseId);
          BatchReadOnlyTransaction batchReadOnlyTransaction =
              batchClient.batchReadOnlyTransaction(spannerConnection.getReadOnlyStaleness());
          try {
            List<Partition> partitions =
                batchReadOnlyTransaction.partitionQuery(
                    PartitionOptions.newBuilder().setMaxPartitions(MAX_PARTITIONS).build(),
                    statement);
            if (partitions.size() < 2) {
              // No need for the extra complexity of a partitioned query.
              result.set(spannerConnection.execute(statement));
            } else {
              // Get the metadata of the query, so we can include that in the result.
              ResultSet metadataResultSet =
                  spannerConnection.analyzeQuery(statement, QueryAnalyzeMode.PLAN);
              result.set(
                  new PartitionQueryResult(
                      batchReadOnlyTransaction.getBatchTransactionId(),
                      partitions,
                      metadataResultSet));
            }
          } catch (SpannerException spannerException) {
            // The query might not be suitable for partitioning. Just try with a normal query.
            result.set(spannerConnection.execute(statement));
          }
        }
      } catch (Exception exception) {
        throw setAndReturn(result, exception);
      }
    }
  }

  private static final ListeningExecutorService DATA_RECEIVER_EXECUTOR =
      MoreExecutors.newDirectExecutorService();

  /**
   * This statement represents a COPY table FROM STDIN statement. This has no one-on-one mapping
   * with a Cloud Spanner SQL statement and is therefore executed using a custom {@link
   * MutationWriter}. As the COPY implementation uses mutations instead of DML, it has slightly
   * different transaction semantics than in real PostgreSQL. A COPY operation will by default be
   * atomic, but can be configured to behave non-atomically for large batches. Also, if a COPY
   * operation is executed in a transaction (both implicit and explicit), it will commit the
   * transaction when the COPY operation is done. This is required to flush the mutations to the
   * database.
   */
  private final class Copy extends BufferedStatement<StatementResult> {
    private final CopyDataReceiver copyDataReceiver;
    private final MutationWriter mutationWriter;
    private final ListeningExecutorService executor;

    Copy(
        ParsedStatement parsedStatement,
        Statement statement,
        CopyDataReceiver copyDataReceiver,
        MutationWriter mutationWriter,
        ExecutorService executor) {
      super(parsedStatement, statement);
      this.copyDataReceiver = copyDataReceiver;
      this.mutationWriter = mutationWriter;
      this.executor = MoreExecutors.listeningDecorator(executor);
    }

    @Override
    boolean isUpdate() {
      return true;
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        // Execute the MutationWriter and the CopyDataReceiver both asynchronously and wait for both
        // to finish before continuing with the next statement. This ensures that all statements are
        // applied in sequential order.
        ListenableFuture<StatementResult> statementResultFuture = executor.submit(mutationWriter);
        this.result.setFuture(statementResultFuture);
        ListenableFuture<Void> copyDataReceiverFuture =
            DATA_RECEIVER_EXECUTOR.submit(copyDataReceiver);

        // Make sure both the front-end CopyDataReceiver and the backend MutationWriter processes
        // have finished before we proceed.
        //noinspection UnstableApiUsage
        Futures.successfulAsList(copyDataReceiverFuture, statementResultFuture).get();
        //noinspection UnstableApiUsage
        Futures.allAsList(copyDataReceiverFuture, statementResultFuture).get();
      } catch (ExecutionException executionException) {
        result.setException(executionException.getCause());
        throw SpannerExceptionFactory.asSpannerException(executionException.getCause());
      } catch (InterruptedException interruptedException) {
        result.setException(PGExceptionFactory.newQueryCancelledException());
        throw PGExceptionFactory.newQueryCancelledException();
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private final class Vacuum extends BufferedStatement<StatementResult> {
    final VacuumStatement vacuumStatement;

    Vacuum(VacuumStatement vacuumStatement) {
      super(vacuumStatement.parsedStatement, vacuumStatement.originalStatement);
      this.vacuumStatement = vacuumStatement;
    }

    @Override
    boolean isUpdate() {
      return false;
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        if (spannerConnection.isInTransaction()) {
          throw PGExceptionFactory.newPGException(
              "VACUUM cannot run inside a transaction block", SQLState.ActiveSqlTransaction);
        }
        for (TableOrIndexName table : vacuumStatement.getTables()) {
          ImmutableList<TableOrIndexName> columns = vacuumStatement.getTableColumns(table);
          Statement statement;
          if (columns == null || columns.isEmpty()) {
            statement = Statement.of("select * from " + table + " limit 1");
          } else {
            statement =
                Statement.of(
                    "select "
                        + columns.stream().map(c -> c.name).collect(Collectors.joining(","))
                        + " from "
                        + table
                        + " limit 1");
          }
          // Just analyze the query to ensure the table and column names are valid.
          spannerConnection.analyzeQuery(statement, QueryAnalyzeMode.PLAN);
        }
        result.set(NO_RESULT);
      } catch (Exception exception) {
        result.setException(toPGException(exception));
        throw exception;
      }
    }
  }

  private final class Truncate extends BufferedStatement<StatementResult> {
    final TruncateStatement truncateStatement;

    Truncate(TruncateStatement truncateStatement) {
      super(truncateStatement.parsedStatement, truncateStatement.originalStatement);
      this.truncateStatement = truncateStatement;
    }

    @Override
    boolean isUpdate() {
      return true;
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        if (spannerConnection.isDdlBatchActive()) {
          throw PGExceptionFactory.newPGException("Cannot execute TRUNCATE in a DDL batch");
        }
        if (spannerConnection.isDmlBatchActive()) {
          throw PGExceptionFactory.newPGException("Cannot execute TRUNCATE in a DML batch");
        }
        if (spannerConnection.isInTransaction()
            || spannerConnection.getAutocommitDmlMode() == AutocommitDmlMode.TRANSACTIONAL) {
          spannerConnection.executeBatchUpdate(truncateStatement.getDeleteStatements());
        } else {
          // BatchDML is not supported for PDML, so we have to loop over the statements.
          // We do not execute the statements in parallel, as there might be dependencies between
          // the tables that are being truncated.
          for (Statement statement : truncateStatement.getDeleteStatements()) {
            spannerConnection.executeUpdate(statement);
          }
        }
        result.set(NO_RESULT);
      } catch (Exception exception) {
        result.setException(exception);
        throw exception;
      }
    }
  }

  private final class Savepoint extends BufferedStatement<StatementResult> {
    private final SavepointStatement savepointStatement;

    Savepoint(SavepointStatement savepointStatement) {
      super(savepointStatement.parsedStatement, savepointStatement.originalStatement);
      this.savepointStatement = savepointStatement;
    }

    @Override
    boolean isUpdate() {
      return false;
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        spannerConnection.savepoint(savepointStatement.getSavepointName());
        result.set(NO_RESULT);
      } catch (Exception exception) {
        PGException pgException =
            PGException.newBuilder(exception).setSQLState(SQLState.SavepointException).build();
        result.setException(pgException);
        throw pgException;
      }
    }
  }

  private final class Release extends BufferedStatement<StatementResult> {
    private final ReleaseStatement releaseStatement;

    Release(ReleaseStatement releaseStatement) {
      super(releaseStatement.parsedStatement, releaseStatement.originalStatement);
      this.releaseStatement = releaseStatement;
    }

    @Override
    boolean isUpdate() {
      return false;
    }

    @Override
    void execute() {
      try {
        checkConnectionState();
        spannerConnection.releaseSavepoint(releaseStatement.getSavepointName());
        result.set(NO_RESULT);
      } catch (Exception exception) {
        PGException pgException =
            PGException.newBuilder(exception).setSQLState(SQLState.SavepointException).build();
        result.setException(pgException);
        throw pgException;
      }
    }
  }

  private final class RollbackTo extends BufferedStatement<StatementResult> {
    private final RollbackToStatement rollbackToStatement;

    RollbackTo(RollbackToStatement rollbackToStatement) {
      super(rollbackToStatement.parsedStatement, rollbackToStatement.originalStatement);
      this.rollbackToStatement = rollbackToStatement;
    }

    @Override
    boolean isUpdate() {
      return false;
    }

    @Override
    void execute() {
      try {
        spannerConnection.rollbackToSavepoint(rollbackToStatement.getSavepointName());
        result.set(NO_RESULT);
      } catch (Exception exception) {
        PGException pgException =
            PGException.newBuilder(exception).setSQLState(SQLState.SavepointException).build();
        result.setException(pgException);
        throw pgException;
      }
    }
  }

  private static final ImmutableMap<String, LocalStatement> EMPTY_LOCAL_STATEMENTS =
      ImmutableMap.of();
  static final StatementResult NO_RESULT = new NoResult();
  private static final StatementResult ROLLBACK_RESULT = new NoResult("ROLLBACK");
  private static final Statement ROLLBACK = Statement.of("ROLLBACK");

  private final Runnable closeAllPortals;
  private final SessionState sessionState;
  private final Supplier<PgCatalog> pgCatalog;
  private final Supplier<ImmutableMap<String, LocalStatement>> localStatements;
  private ConnectionState connectionState = ConnectionState.IDLE;
  private TransactionMode transactionMode = TransactionMode.IMPLICIT;
  private final String currentSchema = "public";
  private final LinkedList<BufferedStatement<?>> bufferedStatements = new LinkedList<>();
  private final Connection spannerConnection;
  private final DatabaseId databaseId;
  private final DdlExecutor ddlExecutor;

  /** Creates a PG backend connection that uses the given Spanner {@link Connection} and options. */
  BackendConnection(
      Runnable closeAllPortals,
      DatabaseId databaseId,
      Connection spannerConnection,
      Supplier<WellKnownClient> wellKnownClient,
      OptionsMetadata optionsMetadata,
      Supplier<ImmutableList<LocalStatement>> localStatements) {
    this.closeAllPortals = closeAllPortals;
    this.sessionState = new SessionState(optionsMetadata);
    this.pgCatalog =
        Suppliers.memoize(
            () -> new PgCatalog(BackendConnection.this.sessionState, wellKnownClient.get()));
    this.spannerConnection = spannerConnection;
    this.databaseId = databaseId;
    this.ddlExecutor =
        new DdlExecutor(this, Suppliers.memoize(() -> wellKnownClient.get().getDdlReplacements()));
    this.localStatements =
        Suppliers.memoize(
            () -> {
              if (localStatements.get().isEmpty()) {
                return EMPTY_LOCAL_STATEMENTS;
              } else {
                Builder<String, LocalStatement> builder = ImmutableMap.builder();
                for (LocalStatement localStatement : localStatements.get()) {
                  for (String sql : localStatement.getSql()) {
                    builder.put(new SimpleImmutableEntry<>(sql, localStatement));
                  }
                }
                return builder.build();
              }
            });
  }

  /** Returns the current connection state. */
  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  /**
   * Buffers the given statement for execution on the backend connection when the next flush/sync
   * message is received. The returned future will contain the result of the statement when
   * execution has finished.
   */
  public Future<StatementResult> execute(
      ParsedStatement parsedStatement,
      Statement statement,
      Function<Statement, Statement> statementBinder) {
    Execute execute = new Execute(parsedStatement, statement, statementBinder);
    bufferedStatements.add(execute);
    return execute.result;
  }

  public ListenableFuture<StatementResult> analyze(
      ParsedStatement parsedStatement, Statement statement) {
    Execute execute = new Execute(parsedStatement, statement, Function.identity(), true);
    bufferedStatements.add(execute);
    return execute.result;
  }

  /**
   * Buffers the given COPY operation for execution on the backend connection when the next
   * flush/sync message is received. The returned future will contain the result of the COPY
   * operation when execution has finished.
   */
  public Future<StatementResult> executeCopy(
      ParsedStatement parsedStatement,
      Statement statement,
      CopyDataReceiver copyDataReceiver,
      MutationWriter mutationWriter,
      ExecutorService executor) {
    Copy copy = new Copy(parsedStatement, statement, copyDataReceiver, mutationWriter, executor);
    bufferedStatements.add(copy);
    return copy.result;
  }

  public Future<StatementResult> executeCopyOut(
      ParsedStatement parsedStatement, Statement statement) {
    CopyOut copyOut = new CopyOut(parsedStatement, statement);
    bufferedStatements.add(copyOut);
    return copyOut.result;
  }

  public Future<StatementResult> execute(VacuumStatement vacuumStatement) {
    Vacuum vacuum = new Vacuum(vacuumStatement);
    bufferedStatements.add(vacuum);
    return vacuum.result;
  }

  public Future<StatementResult> execute(TruncateStatement truncateStatement) {
    Truncate truncate = new Truncate(truncateStatement);
    bufferedStatements.add(truncate);
    return truncate.result;
  }

  public Future<StatementResult> execute(SavepointStatement savepointStatement) {
    Savepoint savepoint = new Savepoint(savepointStatement);
    bufferedStatements.add(savepoint);
    return savepoint.result;
  }

  public Future<StatementResult> execute(ReleaseStatement releaseStatement) {
    Release savepoint = new Release(releaseStatement);
    bufferedStatements.add(savepoint);
    return savepoint.result;
  }

  public Future<StatementResult> execute(RollbackToStatement rollbackToStatement) {
    RollbackTo savepoint = new RollbackTo(rollbackToStatement);
    bufferedStatements.add(savepoint);
    return savepoint.result;
  }

  /** Flushes the buffered statements to Spanner. */
  void flush() {
    flush(false);
  }

  /**
   * Flushes the buffered statements to Spanner and commits/rollbacks the implicit transaction (if
   * any).
   */
  void sync() {
    try {
      flush(true);
    } finally {
      endImplicitTransaction();
    }
  }

  /**
   * Sets the initial value of a pg_settings setting for this connection. This method should only be
   * called during startup with values that come from the connection request.
   */
  public void initSessionSetting(String name, String value) {
    AbstractStatementParser statementParser =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
    if ("options".equalsIgnoreCase(name)) {
      String[] commands = value.split("-c\\s+");
      for (String command : commands) {
        // Special case: If the setting is one that is handled by the Connection API, then we need
        // to execute the statement on the connection instead.
        try {
          ParsedStatement parsedStatement = statementParser.parse(Statement.of("set " + command));
          if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
            this.spannerConnection.execute(Statement.of(parsedStatement.getSqlWithoutComments()));
            continue;
          }
        } catch (Throwable ignore) {
          // Ignore any exceptions during the potential execution of the SET command on the
          // connection and continue with just setting it as session state.
        }
        String[] keyValue = command.split("=", 2);
        if (keyValue.length == 2) {
          SimpleParser parser = new SimpleParser(keyValue[0]);
          TableOrIndexName key = parser.readTableOrIndexName();
          if (key == null) {
            continue;
          }
          this.sessionState.setConnectionStartupValue(key.schema, key.name, keyValue[1].trim());
        }
      }
    } else {
      SimpleParser parser = new SimpleParser(name);
      TableOrIndexName key = parser.readTableOrIndexName();
      if (key == null) {
        return;
      }
      this.sessionState.setConnectionStartupValue(key.schema, key.name, value);
    }
  }

  /** Returns the Spanner connection used by this {@link BackendConnection}. */
  public Connection getSpannerConnection() {
    return this.spannerConnection;
  }

  /** Returns the current schema that is used by this {@link BackendConnection}. */
  public String getCurrentSchema() {
    return this.currentSchema;
  }

  /** Returns the id of the database that this connection uses. */
  public String getCurrentDatabase() {
    return this.databaseId.getDatabase();
  }

  /** Returns the session state of this connection. */
  public SessionState getSessionState() {
    return this.sessionState;
  }

  /**
   * Flushes all buffered statements to Cloud Spanner and commits/rolls back the transaction at the
   * end if isSync=true.
   */
  private void flush(boolean isSync) {
    int index = 0;
    try {
      while (index < bufferedStatements.size()) {
        BufferedStatement<?> bufferedStatement = bufferedStatements.get(index);
        maybeBeginImplicitTransaction(index, isSync);
        // Prepare the connection for executing a DDL statement. This could include committing the
        // current transaction, depending on the settings for execute DDL in transactions.
        if (bufferedStatement.parsedStatement.isDdl()) {
          prepareExecuteDdl(bufferedStatement);
        } else if (transactionMode == TransactionMode.DDL_BATCH && !isTransactionStatement(index)) {
          // End the automatically created DDL batch and revert to an explicit transaction.
          try {
            spannerConnection.runBatch();
          } catch (Exception exception) {
            // Register the exception on the current statement, even though it was caused by a
            // previous one.
            bufferedStatements.get(index).result.setException(exception);
            throw exception;
          } finally {
            transactionMode = TransactionMode.EXPLICIT;
          }
          spannerConnection.beginTransaction();
        }
        boolean canUseBatch = false;
        if (!spannerConnection.isDdlBatchActive()
            && !spannerConnection.isDmlBatchActive()
            && bufferedStatement.isBatchingPossible()
            && index < (getStatementCount() - 1)) {
          StatementType statementType = getStatementType(index);
          StatementType nextStatementType = getStatementType(index + 1);
          canUseBatch = canBeBatchedTogether(statementType, nextStatementType);
        }

        if (canUseBatch) {
          index += executeStatementsInBatch(index);
        } else {
          bufferedStatement.execute();

          if (isBegin(index)) {
            transactionMode = TransactionMode.EXPLICIT;
            connectionState = ConnectionState.TRANSACTION;
          } else if (isCommit(index) || isRollback(index)) {
            if (isCommit(index)) {
              sessionState.commit();
            } else {
              sessionState.rollback();
            }
            closeAllPortals.run();
            transactionMode = TransactionMode.IMPLICIT;
            connectionState = ConnectionState.IDLE;
          }
          index++;
        }
      }
    } catch (Exception exception) {
      connectionState = ConnectionState.ABORTED;
      closeAllPortals.run();
      sessionState.rollback();
      if (spannerConnection.isInTransaction()) {
        if (spannerConnection.isDmlBatchActive()) {
          spannerConnection.abortBatch();
        }
        spannerConnection.setStatementTag(null);
        spannerConnection.execute(ROLLBACK);
      } else if (spannerConnection.isDdlBatchActive()) {
        spannerConnection.abortBatch();
      }
    } finally {
      bufferedStatements.clear();
    }
  }

  /** Starts an implicit transaction if that is necessary. */
  private void maybeBeginImplicitTransaction(int index, boolean isSync) {
    if (connectionState != ConnectionState.IDLE) {
      return;
    }

    // Only start an implicit transaction if we have more than one statement left. Otherwise, just
    // let the Spanner connection execute the statement in auto-commit mode.
    if (isSync && index == bufferedStatements.size() - 1) {
      return;
    }
    // Don't start an implicit transaction if this is already a transaction statement.
    if (isTransactionStatement(index)) {
      return;
    }
    // No need to start a transaction for DDL or client side statements.
    if (bufferedStatements.get(index).parsedStatement.getType() == StatementType.DDL
        || bufferedStatements.get(index).parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      return;
    }
    // If there are only DML statements left, those can be executed as an auto-commit dml batch.
    if (isSync && hasOnlyDmlStatementsAfter(index)) {
      return;
    }
    // Do not start a transaction if a batch is already active.
    if (spannerConnection.isDdlBatchActive() || spannerConnection.isDmlBatchActive()) {
      return;
    }
    // Do not start an implicit transaction if all that is in the buffer is a DESCRIBE and an
    // EXECUTE message for the same statement.
    if (isSync
        && bufferedStatements.size() == 2
        && bufferedStatements.get(0) instanceof Execute
        && bufferedStatements.get(1) instanceof Execute
        && ((Execute) bufferedStatements.get(0)).analyze
        && !((Execute) bufferedStatements.get(1)).analyze
        && bufferedStatements
            .get(0)
            .statement
            .getSql()
            .equals(bufferedStatements.get(1).statement.getSql())) {
      return;
    }

    // We need to start an implicit transaction.
    // Check if a read-only transaction suffices.
    spannerConnection.beginTransaction();
    if (isSync && !hasUpdateStatementsAfter(index)) {
      spannerConnection.setTransactionMode(
          com.google.cloud.spanner.connection.TransactionMode.READ_ONLY_TRANSACTION);
    }
    transactionMode = TransactionMode.IMPLICIT;
    connectionState = ConnectionState.TRANSACTION;
  }

  /** Ends the current implicit transaction (if any). */
  private void endImplicitTransaction() {
    // Only touch the transaction if it is an implicit transaction.
    if (transactionMode != TransactionMode.IMPLICIT) {
      return;
    }

    try {
      if (connectionState != ConnectionState.ABORTED) {
        sessionState.commit();
      }
      if (spannerConnection.isInTransaction()) {
        spannerConnection.setStatementTag(null);
        if (connectionState == ConnectionState.ABORTED) {
          spannerConnection.rollback();
        } else {
          spannerConnection.commit();
        }
      }
    } finally {
      connectionState = ConnectionState.IDLE;
    }
  }

  /**
   * Prepares the connection for executing a DDL statement. This can include committing the current
   * transaction.
   *
   * <p>Executing the DDL statement may or may not be allowed depending on the state of the
   * transaction and the selected {@link
   * com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode}. The method
   * will throw a {@link SpannerException} if executing a DDL statement at this point is not
   * allowed.
   */
  private void prepareExecuteDdl(BufferedStatement<?> bufferedStatement) {
    DdlTransactionMode ddlTransactionMode = sessionState.getDdlTransactionMode();
    try {
      // Single statements are simpler to check, so we do that in a separate check.
      if (bufferedStatements.size() == 1) {
        switch (ddlTransactionMode) {
          case Single:
          case Batch:
          case AutocommitImplicitTransaction:
            // Single DDL statements outside explicit transactions are always allowed. For a single
            // statement, there can also not be an implicit transaction that needs to be committed.
            if (transactionMode == TransactionMode.EXPLICIT) {
              throw PGExceptionFactory.newPGException(
                  "DDL statements are only allowed outside explicit transactions.",
                  SQLState.InvalidTransactionState);
            }
            // Fall-through to commit the transaction if necessary.
          case AutocommitExplicitTransaction:
            // DDL statements are allowed even in explicit transactions. Commit any transaction that
            // might be active.
            if (spannerConnection.isInTransaction()) {
              spannerConnection.commit();
              sessionState.commit();
              if (transactionMode == TransactionMode.EXPLICIT) {
                // Convert the explicit transaction to a DDL batch.
                transactionMode = TransactionMode.DDL_BATCH;
                spannerConnection.startBatchDdl();
              } else {
                transactionMode = TransactionMode.IMPLICIT;
              }
            }
        }
        return;
      }

      // We are in a batch of statements.
      switch (ddlTransactionMode) {
        case Single:
          throw PGExceptionFactory.newPGException(
              "DDL statements are only allowed outside batches and transactions.",
              SQLState.InvalidTransactionState);
        case Batch:
          if (spannerConnection.isInTransaction()
              || bufferedStatements.stream()
                  .anyMatch(
                      statement -> !isStatementAllowedInDdlBatch(statement.parsedStatement))) {
            throw PGExceptionFactory.newPGException(
                "DDL statements are not allowed in mixed batches or transactions.",
                SQLState.InvalidTransactionState);
          }
          break;
        case AutocommitImplicitTransaction:
          if (spannerConnection.isInTransaction() && transactionMode != TransactionMode.IMPLICIT) {
            throw PGExceptionFactory.newPGException(
                "DDL statements are only allowed outside explicit transactions.",
                SQLState.InvalidTransactionState);
          }
          // Fallthrough to commit the transaction if necessary.
        case AutocommitExplicitTransaction:
          // Commit any transaction that might be active and allow executing the statement.
          // Switch the execution state to implicit transaction.
          if (spannerConnection.isInTransaction()) {
            spannerConnection.commit();
            sessionState.commit();
            if (transactionMode == TransactionMode.EXPLICIT) {
              // Convert the explicit transaction to a DDL batch.
              transactionMode = TransactionMode.DDL_BATCH;
              spannerConnection.startBatchDdl();
            } else {
              transactionMode = TransactionMode.IMPLICIT;
            }
          }
      }
    } catch (Throwable throwable) {
      throw setAndReturn(bufferedStatement.result, throwable);
    }
  }

  private boolean isTransactionStatement(ParsedStatement parsedStatement) {
    return isBegin(parsedStatement) || isCommit(parsedStatement) || isRollback(parsedStatement);
  }

  private static final ImmutableSet<ClientSideStatementType> DDL_BATCH_STATEMENTS =
      ImmutableSet.of(
          ClientSideStatementType.START_BATCH_DDL,
          ClientSideStatementType.RUN_BATCH,
          ClientSideStatementType.ABORT_BATCH);

  private boolean isStatementAllowedInDdlBatch(ParsedStatement parsedStatement) {
    return parsedStatement.isDdl()
        || (parsedStatement.getType() == StatementType.CLIENT_SIDE
            && DDL_BATCH_STATEMENTS.contains(parsedStatement.getClientSideStatementType()));
  }

  private boolean isBegin(int index) {
    return isBegin(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isBegin(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.BEGIN;
  }

  private boolean isCommit(int index) {
    return isCommit(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isCommit(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.COMMIT;
  }

  private boolean isRollback(int index) {
    return isRollback(bufferedStatements.get(index).parsedStatement);
  }

  private boolean isRollback(ParsedStatement parsedStatement) {
    return parsedStatement.getType() == StatementType.CLIENT_SIDE
        && parsedStatement.getClientSideStatementType() == ClientSideStatementType.ROLLBACK;
  }

  private boolean isTransactionStatement(int index) {
    return isBegin(index) || isCommit(index) || isRollback(index);
  }

  private boolean hasOnlyDmlStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .allMatch(statement -> statement.parsedStatement.getType() == StatementType.UPDATE);
  }

  @VisibleForTesting
  boolean hasUpdateStatementsAfter(int index) {
    return bufferedStatements.subList(index, bufferedStatements.size()).stream()
        .anyMatch(BufferedStatement::isUpdate);
  }

  private int getStatementCount() {
    return bufferedStatements.size();
  }

  private StatementType getStatementType(int index) {
    return bufferedStatements.get(index).parsedStatement.getType();
  }

  private boolean canBeBatchedTogether(StatementType statementType1, StatementType statementType2) {
    if (Objects.equals(statementType1, StatementType.DDL)
        || Objects.equals(statementType2, StatementType.UPDATE)) {
      return Objects.equals(statementType1, statementType2);
    }
    return false;
  }

  /**
   * Executes the statements from fromIndex in a DML/DDL batch. The batch will consist of all
   * statements from fromIndex till the first statement that is of a different type than the
   * statement at fromIndex. That is; If the first statement is a DML statement, the batch will
   * contain all statements that follow until it encounters a statement that is not a DML statement.
   * The same also applies to DDL statements. Query statements and other statements can not be
   * batched.
   *
   * @param fromIndex The index of the statements array where the batch should start
   * @return The number of statements included in the batch.
   */
  @VisibleForTesting
  int executeStatementsInBatch(int fromIndex) {
    Preconditions.checkArgument(fromIndex < getStatementCount() - 1);
    Preconditions.checkArgument(
        canBeBatchedTogether(getStatementType(fromIndex), getStatementType(fromIndex + 1)));
    StatementType batchType = getStatementType(fromIndex);
    if (batchType == StatementType.UPDATE) {
      spannerConnection.startBatchDml();
    } else if (batchType == StatementType.DDL) {
      spannerConnection.startBatchDdl();
    }
    List<StatementResult> statementResults = new ArrayList<>(getStatementCount());
    int index = fromIndex;
    try {
      while (index < getStatementCount()) {
        if (!bufferedStatements.get(index).isBatchingPossible()) {
          break;
        }
        StatementType statementType = getStatementType(index);
        if (canBeBatchedTogether(batchType, statementType)) {
          // Send DDL statements to the DdlExecutor instead of executing them directly on the
          // connection, so we can support certain DDL constructs that are currently not supported
          // by the backend, such as IF [NOT] EXISTS.
          if (batchType == StatementType.DDL) {
            statementResults.add(
                ddlExecutor.execute(
                    bufferedStatements.get(index).parsedStatement,
                    bufferedStatements.get(index).statement));
          } else {
            Execute execute = (Execute) bufferedStatements.get(index);
            execute.analyzeOrExecute(execute.bindStatement(execute.statement, null));
          }
          index++;
        } else {
          // End the batch here, as the statement type on this index can not be batched together
          // with the other statements in the batch.
          break;
        }
      }
    } catch (Exception exception) {
      // This should normally not happen, as we are not sending any statements to Cloud Spanner yet,
      // but is done as safety precaution to ensure that there is always at least one result.
      // Register the exception on the first statement in the batch.
      bufferedStatements.get(fromIndex).result.setException(exception);
      throw exception;
    }
    try {
      long[] counts = spannerConnection.runBatch();
      if (batchType == StatementType.DDL) {
        counts = extractDdlUpdateCounts(statementResults, counts);
      }
      updateBatchResultCount(fromIndex, counts);
    } catch (SpannerBatchUpdateException batchUpdateException) {
      long[] counts;
      if (batchType == StatementType.DDL) {
        counts = extractDdlUpdateCounts(statementResults, batchUpdateException.getUpdateCounts());
      } else {
        counts = batchUpdateException.getUpdateCounts();
      }
      updateBatchResultCount(fromIndex, counts);
      Execute failedExecute = (Execute) bufferedStatements.get(fromIndex + counts.length);
      failedExecute.result.setException(batchUpdateException);
      throw batchUpdateException;
    } catch (Throwable exception) {
      bufferedStatements.get(fromIndex).result.setException(exception);
      throw exception;
    }
    return index - fromIndex;
  }

  /** Extracts the update count for a list of DDL statements. */
  static long[] extractDdlUpdateCounts(
      List<StatementResult> statementResults, long[] returnedUpdateCounts) {
    int successfullyExecutedCount = 0;
    while (successfullyExecutedCount < returnedUpdateCounts.length
        && returnedUpdateCounts[successfullyExecutedCount] == 1
        && successfullyExecutedCount < statementResults.size()) {
      successfullyExecutedCount++;
    }
    long[] updateCounts = new long[successfullyExecutedCount];
    Arrays.fill(updateCounts, 1L);
    return updateCounts;
  }

  /** Updates the results of the buffered statements after finishing executing a batch. */
  private void updateBatchResultCount(int fromIndex, long[] updateCounts) {
    for (int index = fromIndex; index < fromIndex + updateCounts.length; index++) {
      Execute execute = (Execute) bufferedStatements.get(index);
      if (execute.parsedStatement.getType() == StatementType.DDL) {
        execute.result.set(new NoResult());
      } else {
        execute.result.set(new UpdateCount(updateCounts[index - fromIndex]));
      }
    }
  }

  /**
   * {@link StatementResult} implementation for statements that do not return anything (e.g. DDL).
   */
  @InternalApi
  public static final class NoResult implements StatementResult {
    private final String commandTag;

    NoResult() {
      this.commandTag = null;
    }

    public NoResult(String commandTag) {
      this.commandTag = commandTag;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.NO_RESULT;
    }

    public boolean hasCommandTag() {
      return this.commandTag != null;
    }

    public String getCommandTag() {
      return this.commandTag;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Implementation of {@link StatementResult} for statements that return an update count (e.g.
   * DML).
   */
  @InternalApi
  public static final class UpdateCount implements StatementResult {
    private final Long updateCount;

    public UpdateCount(Long updateCount) {
      this.updateCount = updateCount;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.UPDATE_COUNT;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateCount() {
      return updateCount;
    }
  }

  @InternalApi
  public static final class QueryResult implements StatementResult {
    private final ResultSet resultSet;

    public QueryResult(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.RESULT_SET;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      return resultSet;
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }

  @InternalApi
  public static final class PartitionQueryResult implements StatementResult {
    private final BatchTransactionId batchTransactionId;
    private final List<Partition> partitions;
    private final ResultSet metadataResultSet;

    public PartitionQueryResult(
        BatchTransactionId batchTransactionId,
        List<Partition> partitions,
        ResultSet metadataResultSet) {
      this.batchTransactionId = batchTransactionId;
      this.partitions = partitions;
      this.metadataResultSet = metadataResultSet;
    }

    public BatchTransactionId getBatchTransactionId() {
      return batchTransactionId;
    }

    public List<Partition> getPartitions() {
      return partitions;
    }

    public ResultSet getMetadataResultSet() {
      return metadataResultSet;
    }

    @Override
    public ResultType getResultType() {
      return ResultType.RESULT_SET;
    }

    @Override
    public ClientSideStatementType getClientSideStatementType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getResultSet() {
      return ClientSideResultSet.forRows(
          Type.struct(StructField.of("partition", Type.bytes())),
          partitions.stream()
              .map(
                  partition -> {
                    try {
                      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                      ObjectOutputStream objectOutputStream =
                          new ObjectOutputStream(byteArrayOutputStream);
                      objectOutputStream.writeObject(partition);
                      return Struct.newBuilder()
                          .set("partition")
                          .to(ByteArray.copyFrom(byteArrayOutputStream.toByteArray()))
                          .build();
                    } catch (IOException ioException) {
                      return Struct.newBuilder().set("partition").to((ByteArray) null).build();
                    }
                  })
              .collect(Collectors.toList()));
    }

    @Override
    public Long getUpdateCount() {
      throw new UnsupportedOperationException();
    }
  }
}
