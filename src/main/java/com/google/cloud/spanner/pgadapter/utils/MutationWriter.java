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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.SpannerCallContextTimeoutConfigurator;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.pgadapter.parsers.copy.CopyTreeParser.CopyOptions;
import com.google.cloud.spanner.pgadapter.session.CopySettings;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection.UpdateCount;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Context;
import java.io.Closeable;
import java.io.IOException;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVFormat;
import org.threeten.bp.Duration;

@InternalApi
public class MutationWriter implements Callable<StatementResult>, Closeable {
  public enum CopyTransactionMode {
    /**
     * 'Normal' auto-commit mode. The entire COPY operation is atomic. If the number of mutations
     * exceeds any of the transaction limits of Cloud Spanner, the COPY operation will fail.
     */
    ImplicitAtomic,
    /**
     * The COPY operation is executed as a series of (partly parallel) transactions. The COPY
     * operation is not atomic, and an error halfway the operation can mean that some data was
     * persisted to the database.
     */
    ImplicitNonAtomic,
    /**
     * There is an explicit transaction on the connection. The COPY will be part of that
     * transaction.
     */
    Explicit,
  }

  private static final Logger logger = Logger.getLogger(MutationWriter.class.getName());

  private final CopyTransactionMode transactionMode;
  private long rowCount;
  private final Connection connection;
  private final String tableName;
  private final Map<String, Type> tableColumns;
  private final int maxAtomicBatchSize;
  private final int nonAtomicBatchSize;
  private final long commitSizeLimitForBatching;
  private final CopySettings copySettings;
  private final CopyInParser parser;
  private final PipedOutputStream payload = new PipedOutputStream();
  private final AtomicBoolean commit = new AtomicBoolean(false);
  private final AtomicBoolean rollback = new AtomicBoolean(false);
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

  private final Object lock = new Object();

  @GuardedBy("lock")
  private SpannerException exception;

  public MutationWriter(
      SessionState sessionState,
      CopyTransactionMode transactionMode,
      Connection connection,
      String tableName,
      Map<String, Type> tableColumns,
      int indexedColumnsCount,
      CopyOptions.Format copyFormat,
      CSVFormat format,
      boolean hasHeader)
      throws IOException {
    this.transactionMode = transactionMode;
    this.connection = connection;
    this.tableName = tableName;
    this.tableColumns = tableColumns;
    this.copySettings = new CopySettings(sessionState);
    int atomicMutationLimit = copySettings.getMaxAtomicMutationsLimit();
    this.maxAtomicBatchSize =
        Math.max(atomicMutationLimit / (tableColumns.size() + indexedColumnsCount), 1);
    int nonAtomicMutations = copySettings.getNonAtomicBatchSize();
    this.nonAtomicBatchSize =
        Math.max(nonAtomicMutations / (tableColumns.size() + indexedColumnsCount), 1);
    this.commitSizeLimitForBatching =
        Math.round(
            (float) copySettings.getMaxAtomicCommitSize() / copySettings.getCommitSizeMultiplier());
    this.parser =
        CopyInParser.create(
            copyFormat, format, payload, copySettings.getPipeBufferSize(), hasHeader);
  }

  /** @return number of rows copied into Spanner */
  public long getRowCount() {
    return this.rowCount;
  }

  public void addCopyData(byte[] payload) {
    synchronized (lock) {
      if (this.exception != null) {
        throw this.exception;
      }
    }
    try {
      this.payload.write(payload);
    } catch (IOException e) {
      // Ignore the exception if the executor has already been shutdown. That means that an error
      // occurred that ended the COPY operation while we were writing data to the buffer.
      if (!executorService.isShutdown()) {
        SpannerException spannerException =
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INTERNAL, "Could not write copy data to buffer", e);
        logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
        throw spannerException;
      }
    }
  }

  /** Indicate that this mutation writer should commit. */
  public void commit() {
    this.commit.set(true);
  }

  /**
   * Indicate that this mutation writer should be rolled back. This will not rollback any changes
   * that have already been committed if the mutation writer is running in {@link
   * CopyTransactionMode#ImplicitNonAtomic}.
   */
  public void rollback() {
    this.rollback.set(true);
  }

  @Override
  public void close() throws IOException {
    this.payload.close();
    this.closedLatch.countDown();
  }

  @Override
  public StatementResult call() throws Exception {
    // This LinkedBlockingDeque holds a reference to all transactions that are currently active. The
    // max capacity of this deque is what ensures that we never have more than maxParallelism
    // transactions running at the same time. We could also achieve that by using a thread pool with
    // a fixed number of threads. The problem with that is however that Java does not have a thread
    // pool implementation that will block if a new task is offered and all threads are currently in
    // use. The only options are 'fail or add to queue'. We want to block our worker thread in this
    // case when the max parallel transactions has been reached, as that automatically creates back-
    // pressure in our entire pipeline that consists of:
    // Client app (psql) -> CopyData message -> CSVParser -> Transaction.
    LinkedBlockingDeque<ApiFuture<Void>> activeCommitFutures =
        new LinkedBlockingDeque<>(copySettings.getMaxParallelism());
    // This list holds all transactions that we have started. We will wait on this entire list
    // before finishing, to ensure that all data has been written before we signal that we are done.
    List<ApiFuture<Void>> allCommitFutures = new ArrayList<>();
    try {
      Iterator<CopyRecord> iterator = this.parser.iterator();
      List<Mutation> mutations = new ArrayList<>();
      long currentBufferByteSize = 0L;
      // Note: iterator.hasNext() blocks if there is not enough data in the pipeline to construct a
      // complete record. It returns false if the stream has been closed and all records have been
      // returned.
      while (!rollback.get() && iterator.hasNext()) {
        CopyRecord record = iterator.next();
        if (record.numColumns() != this.tableColumns.keySet().size()) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              "Invalid COPY data: Row length mismatched. Expected "
                  + this.tableColumns.keySet().size()
                  + " columns, but only found "
                  + record.numColumns());
        }

        Mutation mutation = buildMutation(record);
        int mutationSize = calculateSize(mutation);
        this.rowCount++;

        if (transactionMode == CopyTransactionMode.ImplicitNonAtomic) {
          currentBufferByteSize =
              addMutationAndMaybeFlushTransaction(
                  activeCommitFutures,
                  allCommitFutures,
                  mutations,
                  mutation,
                  currentBufferByteSize,
                  mutationSize);
        } else {
          mutations.add(mutation);
          currentBufferByteSize += mutationSize;
          if (mutations.size() > maxAtomicBatchSize) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "Record count: "
                    + mutations.size()
                    + " has exceeded the limit: "
                    + maxAtomicBatchSize
                    + ".\n\nThe number of mutations per record is equal to the number of columns in the record "
                    + "plus the number of indexed columns in the record. The maximum number of mutations "
                    + "in one transaction is "
                    + copySettings.getMaxAtomicMutationsLimit()
                    + ".\n\nExecute `SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation "
                    + "to instruct PGAdapter to automatically break large transactions into multiple smaller. "
                    + "This will make the COPY operation non-atomic.\n\n");
          }
          if (currentBufferByteSize > copySettings.getMaxAtomicCommitSize()) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "Commit size: "
                    + currentBufferByteSize
                    + " has exceeded the limit: "
                    + copySettings.getMaxAtomicCommitSize()
                    + ".\n\nExecute `SET SPANNER.AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation "
                    + "to instruct PGAdapter to automatically break large transactions into multiple smaller. "
                    + "This will make the COPY operation non-atomic.\n\n");
          }
        }
      } // end of iterator.hasNext()

      // There are no more CSVRecords in the pipeline.
      // Write any remaining mutations in the buffer.
      if (!rollback.get() && !mutations.isEmpty()) {
        if (transactionMode == CopyTransactionMode.Explicit) {
          connection.write(mutations);
        } else {
          // Wait until we have received a CopyDone message before writing the remaining data to
          // Spanner. If we are in a non-atomic transaction, there might already be data that have
          // been written to Spanner.
          closedLatch.await();
          if (commit.get()) {
            allCommitFutures.add(writeToSpannerAsync(activeCommitFutures, mutations));
          }
        }
      }
      // Wait for all commits to finish. We do this even if something went wrong, as it ensures two
      // things:
      // 1. All commits that were in flight when something went wrong will finish before we return
      //    an error to the client application. This prevents commits still being added to the
      //    database after we have returned an error, which could cause confusion.
      // 2. This will throw the underlying exception, so we can catch and register it.
      ApiFutures.allAsList(allCommitFutures).get();
    } catch (SpannerException e) {
      synchronized (lock) {
        this.exception = e;
        throw this.exception;
      }
    } catch (ExecutionException e) {
      synchronized (lock) {
        this.exception = SpannerExceptionFactory.asSpannerException(e.getCause());
        throw this.exception;
      }
    } catch (Exception e) {
      synchronized (lock) {
        this.exception = SpannerExceptionFactory.asSpannerException(e);
        throw this.exception;
      }
    } finally {
      this.executorService.shutdown();
      if (!this.executorService.awaitTermination(60L, TimeUnit.SECONDS)) {
        logger.log(Level.WARNING, "Timeout while waiting for MutationWriter executor to shutdown.");
      }
      this.payload.close();
      this.parser.close();
    }
    return new UpdateCount(rowCount);
  }

  private long addMutationAndMaybeFlushTransaction(
      LinkedBlockingDeque<ApiFuture<Void>> activeCommitFutures,
      List<ApiFuture<Void>> allCommitFutures,
      List<Mutation> mutations,
      Mutation mutation,
      long currentBufferByteSize,
      int mutationSize)
      throws Exception {
    // Flush before adding to the buffer if either of the commit size limits would be exceeded.
    long estimatedNextSize = currentBufferByteSize + mutationSize;
    if (!mutations.isEmpty()
        && (estimatedNextSize > commitSizeLimitForBatching
            || estimatedNextSize > copySettings.getMaxNonAtomicCommitSize())) {
      allCommitFutures.add(writeToSpannerAsync(activeCommitFutures, mutations));
      mutations.clear();
      mutations.add(mutation);
      return mutationSize;
    }

    mutations.add(mutation);
    if (mutations.size() == nonAtomicBatchSize) {
      allCommitFutures.add(writeToSpannerAsync(activeCommitFutures, mutations));
      mutations.clear();
      return 0L; // Buffer is empty, so the batch size in bytes is now back to zero.
    }
    return currentBufferByteSize + mutationSize;
  }

  private ApiFuture<Void> writeToSpannerAsync(
      LinkedBlockingDeque<ApiFuture<Void>> activeCommitFutures, Iterable<Mutation> mutations)
      throws Exception {

    SettableApiFuture<Void> settableApiFuture = SettableApiFuture.create();
    // Add this future to the list of active commit futures. This will block if the deque is full,
    // and this will effectively apply back-pressure to the entire stream as the worker thread is
    // blocked until there is room in the deque.
    activeCommitFutures.put(settableApiFuture);

    DatabaseClient dbClient = connection.getDatabaseClient();
    ImmutableList<Mutation> immutableMutations = ImmutableList.copyOf(mutations);
    ListenableFuture<Void> listenableFuture =
        executorService.submit(
            () -> {
              Context context =
                  Context.current()
                      .withValue(
                          SpannerOptions.CALL_CONTEXT_CONFIGURATOR_KEY,
                          SpannerCallContextTimeoutConfigurator.create()
                              .withCommitTimeout(
                                  Duration.ofSeconds(copySettings.getCommitTimeoutSeconds())));
              context.run(() -> dbClient.write(immutableMutations));
              return null;
            });
    Futures.addCallback(
        listenableFuture,
        new FutureCallback<Void>() {
          @Override
          public void onFailure(@Nonnull Throwable t) {
            rollback.set(true);
            //noinspection ResultOfMethodCallIgnored
            activeCommitFutures.remove(settableApiFuture);
            settableApiFuture.setException(t);
          }

          @Override
          public void onSuccess(Void result) {
            //noinspection ResultOfMethodCallIgnored
            activeCommitFutures.remove(settableApiFuture);
            settableApiFuture.set(result);
          }
        },
        MoreExecutors.directExecutor());
    return settableApiFuture;
  }

  static int calculateSize(Mutation mutation) {
    int size = 0;
    for (Value value : mutation.getValues()) {
      if (value.isNull()) {
        size++;
        continue;
      }

      switch (value.getType().getCode()) {
        case BOOL:
          size++;
          break;
        case FLOAT64:
        case INT64:
          size += 8;
          break;
        case PG_NUMERIC:
          size += value.getString().length();
          break;
        case STRING:
        case PG_JSONB:
          // Assume four bytes per character to be on the safe side.
          size += value.getString().length() * 4;
          break;
        case BYTES:
          size += value.getBytes().length();
          break;
        case TIMESTAMP:
          size += 30;
          break;
        case DATE:
          size += 10;
          break;
        case ARRAY:
          switch (value.getType().getArrayElementType().getCode()) {
            case BOOL:
              size += value.getBoolArray().size();
              break;
            case FLOAT64:
              size += value.getFloat64Array().size() * 8;
              break;
            case INT64:
              size += value.getInt64Array().size() * 8;
              break;
            case PG_NUMERIC:
              for (String s : value.getStringArray()) {
                size += s.length();
              }
              break;
            case PG_JSONB:
            case STRING:
              for (String s : value.getStringArray()) {
                size += s.length() * 4;
              }
              break;
            case BYTES:
              for (ByteArray b : value.getBytesArray()) {
                size += b.length();
              }
              break;
            case TIMESTAMP:
              size += value.getTimestampArray().size() * 30;
              break;
            case DATE:
              size += value.getDateArray().size() * 10;
              break;
            case ARRAY:
            case NUMERIC:
            case JSON:
            case STRUCT:
              break;
          }
          break;
        case NUMERIC:
        case JSON:
        case STRUCT:
          break;
      }
    }
    return size;
  }

  @VisibleForTesting
  Mutation buildMutation(CopyRecord record) {
    WriteBuilder builder;
    // The default is to use Insert, but PGAdapter also supports InsertOrUpdate. This can be very
    // useful for importing large datasets using PartitionedNonAtomic mode. If an import attempt
    // fails halfway, it can easily be retried with InsertOrUpdate as it will just overwrite
    // existing records instead of failing on a UniqueKeyConstraint violation.
    if (copySettings.isCopyUpsert()) {
      builder = Mutation.newInsertOrUpdateBuilder(this.tableName);
    } else {
      builder = Mutation.newInsertBuilder(this.tableName);
    }
    // Iterate through all table column to copy into
    int index = 0;
    for (String columnName : this.tableColumns.keySet()) {
      Type columnType = this.tableColumns.get(columnName);
      Value value =
          record.hasColumnNames()
              ? record.getValue(columnType, columnName)
              : record.getValue(columnType, index);
      builder.set(columnName).to(value);
      index++;
    }
    return builder.build();
  }
}
