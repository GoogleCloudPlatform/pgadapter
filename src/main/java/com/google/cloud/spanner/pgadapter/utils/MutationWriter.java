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
import com.google.api.core.SettableApiFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.connection.Connection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spanner.v1.TypeCode;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.postgresql.jdbc.TimestampUtils;

public class MutationWriter implements Callable<Long>, Closeable {
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
  private static final String ERROR_FILE = "output.txt";

  private static final int DEFAULT_MUTATION_LIMIT = 20_000; // 20k mutation count limit
  private static final int DEFAULT_COMMIT_LIMIT =
      100_000_000; // 100MB mutation API commit size limit
  /**
   * The factor that the size of the incoming payload is multiplied with to estimate whether the
   * current commit size will exceed the maximum commit size. The factor is chosen conservatively to
   * ensure that a COPY operation of a large file does not unnecessarily break because of an
   * exceeded commit size.
   */
  private static final float DEFAULT_COMMIT_LIMIT_MULTIPLIER_FACTOR = 2.0f;

  private static final int DEFAULT_MAX_PARALLELISM = 128;
  private static final int DEFAULT_PIPE_BUFFER_SIZE = 1 << 16;

  /**
   * COPY will INSERT records by default. This is consistent with how COPY on PostgreSQL works. This
   * option allows PGAdapter to use InsertOrUpdate instead. This can be slightly more efficient for
   * bulk uploading, and it makes it easier to retry a failed non-atomic batch that might have
   * already uploaded some but not all data.
   */
  private final boolean insertOrUpdate =
      Boolean.parseBoolean(System.getProperty("copy_in_insert_or_update", "false"));

  private final int commitSizeLimit =
      Integer.parseInt(
          System.getProperty("copy_in_commit_limit", String.valueOf(DEFAULT_COMMIT_LIMIT)));
  private final CopyTransactionMode transactionMode;
  private final boolean hasHeader;
  private boolean isHeaderParsed;
  private long rowCount;
  private final Connection connection;
  private DatabaseClient databaseClient;
  private final String tableName;
  private final Map<String, TypeCode> tableColumns;
  private final int maxBatchSize;
  private final long commitSizeLimitForBatching;
  private final int maxParallelism;
  private final int pipeBufferSize;
  private final CSVFormat format;
  private final CSVParser parser;
  private PrintWriter errorFileWriter;
  private final PipedOutputStream payload = new PipedOutputStream();
  private final AtomicBoolean rollback = new AtomicBoolean(false);
  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

  private final Object lock = new Object();

  @GuardedBy("lock")
  private SpannerException exception;

  public MutationWriter(
      CopyTransactionMode transactionMode,
      Connection connection,
      String tableName,
      Map<String, TypeCode> tableColumns,
      int indexedColumnsCount,
      CSVFormat format,
      boolean hasHeader)
      throws IOException {
    this(
        transactionMode,
        connection,
        null,
        tableName,
        tableColumns,
        indexedColumnsCount,
        format,
        hasHeader);
  }

  @VisibleForTesting
  MutationWriter(
      CopyTransactionMode transactionMode,
      Connection connection,
      DatabaseClient databaseClient,
      String tableName,
      Map<String, TypeCode> tableColumns,
      int indexedColumnsCount,
      CSVFormat format,
      boolean hasHeader)
      throws IOException {
    this.transactionMode = transactionMode;
    this.connection = connection;
    this.databaseClient = databaseClient;
    this.hasHeader = hasHeader;
    this.isHeaderParsed = false;
    this.tableName = tableName;
    this.tableColumns = tableColumns;
    int mutationLimit =
        Math.max(
            Integer.parseInt(
                System.getProperty(
                    "copy_in_mutation_limit", String.valueOf(DEFAULT_MUTATION_LIMIT))),
            1);
    this.maxBatchSize = Math.max(mutationLimit / (tableColumns.size() + indexedColumnsCount), 1);
    float commitLimitMultiplierFactor =
        Math.max(
            Float.parseFloat(
                System.getProperty(
                    "copy_in_commit_limit_multiplier_factor",
                    String.valueOf(DEFAULT_COMMIT_LIMIT_MULTIPLIER_FACTOR))),
            1.0f);
    this.commitSizeLimitForBatching =
        Math.round((float) commitSizeLimit / commitLimitMultiplierFactor);
    this.maxParallelism =
        Math.max(
            Integer.parseInt(
                System.getProperty(
                    "copy_in_max_parallelism", String.valueOf(DEFAULT_MAX_PARALLELISM))),
            1);
    this.pipeBufferSize =
        Math.max(
            Integer.parseInt(
                System.getProperty(
                    "copy_in_pipe_buffer_size", String.valueOf(DEFAULT_PIPE_BUFFER_SIZE))),
            1024);
    this.format = format;
    this.parser = createParser();
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
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INTERNAL, "Could not write copy data to buffer", e);
    }
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
  }

  @Override
  public Long call() throws Exception {
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
        new LinkedBlockingDeque<>(maxParallelism);
    // This list holds all transactions that we have started. We will wait on this entire list
    // before finishing, to ensure that all data has been written before we signal that we are done.
    List<ApiFuture<Void>> allCommitFutures = new ArrayList<>();
    try {
      Iterator<CSVRecord> iterator = this.parser.iterator();
      List<Mutation> mutations = new ArrayList<>();
      long currentBufferByteSize = 0L;
      // Note: iterator.hasNext() blocks if there is not enough data in the pipeline to construct a
      // complete record. It returns false if the stream has been closed and all records have been
      // returned.
      while (!rollback.get() && iterator.hasNext()) {
        CSVRecord record = iterator.next();
        if (record.size() != this.tableColumns.keySet().size()) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              "Invalid COPY data: Row length mismatched. Expected "
                  + this.tableColumns.keySet().size()
                  + " columns, but only found "
                  + record.size());
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
          if (mutations.size() > maxBatchSize) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "Record count: "
                    + mutations.size()
                    + " has exceeded the limit: "
                    + maxBatchSize
                    + ".\n\nThe number of mutations per record is equal to the number of columns in the record "
                    + "plus the number of indexed columns in the record. The maximum number of mutations "
                    + "in one transaction is "
                    + DEFAULT_MUTATION_LIMIT
                    + ".\n\nExecute `SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation "
                    + "to instruct PGAdapter to automatically break large transactions into multiple smaller. "
                    + "This will make the COPY operation non-atomic.\n\n");
          }
          if (currentBufferByteSize > commitSizeLimit) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                "Commit size: "
                    + currentBufferByteSize
                    + " has exceeded the limit: "
                    + commitSizeLimit
                    + ".\n\nExecute `SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'` before executing a large COPY operation "
                    + "to instruct PGAdapter to automatically break large transactions into multiple smaller. "
                    + "This will make the COPY operation non-atomic.\n\n");
          }
        }
      } // end of iterator.hasNext()

      // There are no more CSVRecords in the pipeline.
      // Write any remaining mutations in the buffer.
      if (!rollback.get() && !mutations.isEmpty()) {
        if (transactionMode == CopyTransactionMode.Explicit) {
          connection.bufferedWrite(mutations);
        } else {
          allCommitFutures.add(writeToSpannerAsync(activeCommitFutures, mutations));
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
      closeErrorFile();
    }
    return rowCount;
  }

  private long addMutationAndMaybeFlushTransaction(
      LinkedBlockingDeque<ApiFuture<Void>> activeCommitFutures,
      List<ApiFuture<Void>> allCommitFutures,
      List<Mutation> mutations,
      Mutation mutation,
      long currentBufferByteSize,
      int mutationSize)
      throws Exception {
    // Flush before adding to the buffer if the commit size would be exceeded.
    if (!mutations.isEmpty() && currentBufferByteSize + mutationSize > commitSizeLimitForBatching) {
      allCommitFutures.add(writeToSpannerAsync(activeCommitFutures, mutations));
      mutations.clear();
      mutations.add(mutation);
      return mutationSize;
    }

    mutations.add(mutation);
    if (mutations.size() == maxBatchSize) {
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

    DatabaseClient dbClient = getDatabaseClient();
    ImmutableList<Mutation> immutableMutations = ImmutableList.copyOf(mutations);
    ListenableFuture<Void> listenableFuture =
        executorService.submit(
            () -> {
              dbClient.write(immutableMutations);
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

  private DatabaseClient getDatabaseClient() {
    if (databaseClient == null) {
      try {
        // TODO: Replace with connection.getDatabaseClient() when 6.21 has been released.
        Class<?> connectionImplClass =
            Class.forName("com.google.cloud.spanner.connection.ConnectionImpl");
        Field dbClientField = connectionImplClass.getDeclaredField("dbClient");
        dbClientField.setAccessible(true);
        databaseClient = (DatabaseClient) dbClientField.get(this.connection);
      } catch (Exception e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, "Failed to get database client from connection", e);
      }
    }
    return databaseClient;
  }

  private int calculateSize(Mutation mutation) {
    int size = 0;
    for (Value value : mutation.getValues()) {
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
        case JSON:
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
            case INT64:
              size += value.getInt64Array().size() * 8;
              break;
            case PG_NUMERIC:
              for (String s : value.getStringArray()) {
                size += s.length();
              }
              break;
            case JSON:
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
            case STRUCT:
              break;
          }
          break;
        case NUMERIC:
        case STRUCT:
          break;
      }
    }
    return size;
  }

  private Mutation buildMutation(CSVRecord record) {
    TimestampUtils timestampUtils = new TimestampUtils(false, () -> null);
    WriteBuilder builder;
    // The default is to use Insert, but PGAdapter also supports InsertOrUpdate. This can be very
    // useful for importing large datasets using PartitionedNonAtomic mode. If an import attempt
    // fails halfway, it can easily be retried with InsertOrUpdate as it will just overwrite
    // existing records instead of failing on a UniqueKeyConstraint violation.
    if (this.insertOrUpdate) {
      builder = Mutation.newInsertOrUpdateBuilder(this.tableName);
    } else {
      builder = Mutation.newInsertBuilder(this.tableName);
    }
    // Iterate through all table column to copy into
    for (String columnName : this.tableColumns.keySet()) {
      TypeCode columnType = this.tableColumns.get(columnName);
      String recordValue = "";
      try {
        recordValue = record.get(columnName).trim();
        switch (columnType) {
          case STRING:
            builder.set(columnName).to(recordValue);
            break;
          case JSON:
            builder.set(columnName).to(Value.json(recordValue));
            break;
          case BOOL:
            builder.set(columnName).to(Boolean.parseBoolean(recordValue));
            break;
          case INT64:
            builder.set(columnName).to(Long.parseLong(recordValue));
            break;
          case FLOAT64:
            builder.set(columnName).to(Double.parseDouble(recordValue));
            break;
          case NUMERIC:
            builder.set(columnName).to(Value.pgNumeric(recordValue));
            break;
          case BYTES:
            if (recordValue.startsWith("\\x")) {
              builder
                  .set(columnName)
                  .to(ByteArray.copyFrom(Hex.decodeHex(recordValue.substring(2))));
            }
            break;
          case DATE:
            builder.set(columnName).to(Date.parseDate(recordValue));
            break;
          case TIMESTAMP:
            Timestamp timestamp = timestampUtils.toTimestamp(null, recordValue);
            builder.set(columnName).to(com.google.cloud.Timestamp.of(timestamp));
            break;
        }
      } catch (NumberFormatException | DateTimeParseException e) {
        handleError(e);
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid input syntax for type "
                + columnType.toString()
                + ":"
                + "\""
                + recordValue
                + "\"",
            e);
      } catch (IllegalArgumentException e) {
        handleError(e);
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid input syntax for column \"" + columnName + "\"",
            e);
      } catch (Exception e) {
        handleError(e);
        throw SpannerExceptionFactory.asSpannerException(e);
      }
    }
    return builder.build();
  }

  private CSVParser createParser() throws IOException {
    // Construct the CSVParser directly on the stream of incoming CopyData messages, so we don't
    // store more data in memory than necessary. Loading all data into memory first before starting
    // to parse and write the CSVRecords could otherwise cause an out-of-memory exception for large
    // files.
    Reader reader =
        new InputStreamReader(
            new PipedInputStream(this.payload, this.pipeBufferSize), StandardCharsets.UTF_8);
    CSVParser parser;
    if (this.hasHeader && !this.isHeaderParsed) {
      parser = CSVParser.parse(reader, this.format.withFirstRecordAsHeader());
      this.isHeaderParsed = true;
    } else {
      parser = CSVParser.parse(reader, this.format);
    }
    return parser;
  }

  public void handleError(Exception exception) {
    writeErrorFile(exception);
  }

  private void createErrorFile() {
    File unsuccessfulCopy = new File(ERROR_FILE);
    try {
      this.errorFileWriter = new PrintWriter(new FileWriter(unsuccessfulCopy, false));
    } catch (IOException e) {
      throw SpannerExceptionFactory.asSpannerException(e);
    }
  }

  /** Writes any error that occurred during a COPY operation to the error file. */
  public void writeErrorFile(Exception exception) {
    if (this.errorFileWriter == null) {
      createErrorFile();
    }
    exception.printStackTrace(errorFileWriter);
  }

  public void closeErrorFile() {
    if (this.errorFileWriter != null) {
      this.errorFileWriter.close();
    }
  }
}
