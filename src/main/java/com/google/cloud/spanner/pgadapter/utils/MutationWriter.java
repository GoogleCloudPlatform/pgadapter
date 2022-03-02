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

import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.common.collect.Iterables;
import com.google.spanner.v1.TypeCode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MutationWriter {
  public enum CopyTransactionMode {
    ImplicitAtomic,
    ImplicitNonAtomic,
    Explicit,
  }

  private static final int MUTATION_LIMIT = 20_000; // 20k mutation count limit
  private static final long COMMIT_LIMIT = 100_000_000; // 100MB mutation API commit size limit
  private static final String ERROR_FILE = "output.txt";

  /**
   * The factor that the size of the incoming payload is multiplied with to estimate whether the
   * current commit size will exceed the maximum commit size. The factor is chosen conservatively to
   * ensure that a COPY operation of a large file does not unnecessarily break because of an
   * exceeded commit size.
   */
  private static final float COMMIT_LIMIT_MULTIPLIER_FACTOR = 2.0f;

  private final CopyTransactionMode transactionMode;
  private final boolean hasHeader;
  private boolean isHeaderParsed;
  private int mutationCount;
  private int rowCount;
  private List<Mutation> mutations;
  private final String tableName;
  private final Map<String, TypeCode> tableColumns;
  private final int indexedColumnsCount;
  private final CSVFormat format;
  private FileWriter fileWriter;
  private final ByteArrayOutputStream payload = new ByteArrayOutputStream();

  public MutationWriter(
      CopyTransactionMode transactionMode,
      String tableName,
      Map<String, TypeCode> tableColumns,
      int indexedColumnsCount,
      CSVFormat format,
      boolean hasHeader) {
    this.transactionMode = transactionMode;
    this.mutationCount = 0;
    this.hasHeader = hasHeader;
    this.isHeaderParsed = false;
    this.tableName = tableName;
    this.tableColumns = tableColumns;
    this.indexedColumnsCount = indexedColumnsCount;
    this.format = format;
    this.mutations = new ArrayList<>();
  }

  /** @return list of CopyData mutations not yet written into Spanner */
  public List<Mutation> getMutations() {
    return this.mutations;
  }

  /** @return number of rows copied into Spanner */
  public int getRowCount() {
    return this.rowCount;
  }

  public void addCopyData(ConnectionHandler connectionHandler, byte[] payload) throws Exception {
    if (transactionMode == CopyTransactionMode.ImplicitNonAtomic
        && (commitSizeIsWillExceedLimit(payload))) {
      // Flush the current mutations to Spanner and start a new batch.
      buildMutationList(connectionHandler);
      writeToSpanner(connectionHandler);
    }
    this.payload.write(payload, 0, payload.length);
    if (transactionMode != CopyTransactionMode.ImplicitNonAtomic && !commitSizeIsWithinLimit()) {
      handleError(connectionHandler);
      throw new SQLException(
          "Commit size: " + this.payload.size() + " has exceeded the limit: " + COMMIT_LIMIT);
    }
  }

  /** Build mutation to add to mutations list with data contained within a CopyData payload */
  public void buildMutationList(ConnectionHandler connectionHandler) throws Exception {
    List<CSVRecord> records = parsePayloadData(this.payload.toByteArray());
    for (CSVRecord record : records) {
      // Check that the number of columns in a record matches the number of columns in the table
      if (record.size() != this.tableColumns.keySet().size()) {
        handleError(connectionHandler);
        throw new SQLException(
            "Invalid COPY data: Row length mismatched. Expected "
                + this.tableColumns.keySet().size()
                + " columns, but only found "
                + record.size());
      }
      WriteBuilder builder = Mutation.newInsertBuilder(this.tableName);
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
              builder.set(columnName).to(Byte.parseByte(recordValue));
              break;
            case DATE:
              builder.set(columnName).to(Date.parseDate(recordValue));
              break;
            case TIMESTAMP:
              builder.set(columnName).to(com.google.cloud.Timestamp.parseTimestamp(recordValue));
              break;
          }
        } catch (NumberFormatException | DateTimeParseException e) {
          handleError(connectionHandler);
          throw new SQLException(
              "Invalid input syntax for type "
                  + columnType.toString()
                  + ":"
                  + "\""
                  + recordValue
                  + "\"");
        } catch (IllegalArgumentException e) {
          handleError(connectionHandler);
          throw new SQLException("Invalid input syntax for column \"" + columnName + "\"");
        } catch (Exception e) {
          handleError(connectionHandler);
          throw e;
        }
      }
      this.mutations.add(builder.build()); // Add write builder to mutation list
      this.mutationCount += record.size(); // Increment the number of mutations being added
      this.rowCount++; // Increment the number of COPY rows by one
    }
    if (transactionMode != CopyTransactionMode.ImplicitNonAtomic && !mutationCountIsWithinLimit()) {
      handleError(connectionHandler);
      throw new SQLException(
          "Mutation count: " + mutationCount + " has exceeded the limit: " + MUTATION_LIMIT);
    }
  }

  /**
   * @return True if current payload will fit within COMMIT_LIMIT. This is only an estimate and the
   *     actual commit size may still be rejected by Spanner.
   */
  private boolean commitSizeIsWithinLimit() {
    return this.payload.size() <= COMMIT_LIMIT;
  }

  /**
   * @return True if the current payload + next payload will exceed COMMIT_LIMIT. This estimate is
   *     adjusted using the COMMIT_LIMIT_MULTIPLIER_FACTOR to be on the safe side.
   */
  private boolean commitSizeIsWillExceedLimit(byte[] nextPayload) {
    return (this.payload.size() + nextPayload.length) * COMMIT_LIMIT_MULTIPLIER_FACTOR
        >= COMMIT_LIMIT;
  }

  /**
   * @return True if current mutation count will fit within MUTATION_LIMIT. This is only an estimate
   *     and the actual number of mutations may be different which could result in spanner rejecting
   *     the transaction.
   */
  private boolean mutationCountIsWithinLimit() {
    return this.mutationCount <= MUTATION_LIMIT;
  }

  /** @return list of CSVRecord rows parsed with CSVParser from CopyData payload byte array */
  private List<CSVRecord> parsePayloadData(byte[] payload) throws IOException {
    String copyData = new String(payload, StandardCharsets.UTF_8).trim();
    CSVParser parser;
    if (this.hasHeader && !this.isHeaderParsed) {
      parser = CSVParser.parse(copyData, this.format.withFirstRecordAsHeader());
      this.isHeaderParsed = true;
    } else {
      parser = CSVParser.parse(copyData, this.format);
    }
    // Skip the last record if that is the '\.' end of file indicator.
    List<CSVRecord> records = parser.getRecords();
    if (!records.isEmpty()
        && records.get(records.size() - 1).size() == 1
        && "\\.".equals(records.get(records.size() - 1).get(0))) {
      return records.subList(0, records.size() - 1);
    }
    return records;
  }

  /**
   * Write list of mutations to Spanner and reset for next batch
   *
   * @return count of the number of rows updated.
   */
  public int writeToSpanner(ConnectionHandler connectionHandler) throws SQLException {
    if (this.mutations.isEmpty()) {
      return 0;
    }
    Connection connection = connectionHandler.getJdbcConnection();
    CloudSpannerJdbcConnection spannerConnection =
        connection.unwrap(CloudSpannerJdbcConnection.class);
    switch (transactionMode) {
      case ImplicitAtomic:
        spannerConnection.write(this.mutations);
        break;
      case ImplicitNonAtomic:
        batchedWriteToSpanner(spannerConnection);
        break;
      case Explicit:
        spannerConnection.bufferedWrite(this.mutations);
        break;
    }
    // Reset mutations, mutation counter, and batch size count for a new batch
    this.mutations = new ArrayList<>();
    this.mutationCount = 0;
    this.payload.reset();
    return this.rowCount;
  }

  private void batchedWriteToSpanner(CloudSpannerJdbcConnection spannerConnection)
      throws SQLException {
    int batchSize = MUTATION_LIMIT / (tableColumns.size() + indexedColumnsCount);
    for (List<Mutation> mutations : Iterables.partition(this.mutations, batchSize)) {
      spannerConnection.write(mutations);
    }
  }

  public void handleError(ConnectionHandler connectionHandler) throws Exception {
    this.mutations = new ArrayList<>();
    this.mutationCount = 0;
    writeCopyDataToErrorFile();
    this.payload.reset();
  }

  private void createErrorFile() throws IOException {
    File unsuccessfulCopy = new File(ERROR_FILE);
    this.fileWriter = new FileWriter(unsuccessfulCopy, false);
  }

  /**
   * Copy data will be written to an error file if size limits were exceeded or a problem was
   * encountered. Copy data will also be written if an error was encountered while generating the
   * mutation list or if Spanner returns an error upon committing the mutations.
   */
  public void writeCopyDataToErrorFile() throws IOException {
    if (this.fileWriter == null) {
      createErrorFile();
    }
    this.fileWriter.write(
        new String(this.payload.toByteArray(), StandardCharsets.UTF_8).trim() + "\n");
  }

  public void closeErrorFile() throws IOException {
    if (this.fileWriter != null) {
      this.fileWriter.close();
    }
  }
}
