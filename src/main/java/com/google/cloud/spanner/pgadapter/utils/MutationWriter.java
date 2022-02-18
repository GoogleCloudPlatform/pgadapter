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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MutationWriter {

  private static final long MUTATION_LIMIT = 20000; // 20k mutation count limit
  private static final long COMMIT_LIMIT = 100000000; // 100MB mutation API commit size limit
  private static final String ERROR_FILE = "output.txt";

  private boolean hasHeader;
  private boolean isHeaderParsed;
  private int mutationCount;
  private int batchSize;
  private int rowCount;
  private List<Mutation> mutations;
  private String tableName;
  private Map<String, TypeCode> tableColumns;
  private CSVFormat format;
  private FileWriter fileWriter;

  public MutationWriter(
      String tableName, Map<String, TypeCode> tableColumns, CSVFormat format, boolean hasHeader) {
    this.mutationCount = 0;
    this.batchSize = 0;
    this.hasHeader = hasHeader;
    this.isHeaderParsed = false;
    this.tableName = tableName;
    this.tableColumns = tableColumns;
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

  /** Build mutation to add to mutations list with data contained within a CopyData payload */
  public void buildMutation(ConnectionHandler connectionHandler, byte[] payload) throws Exception {
    List<CSVRecord> records = parsePayloadData(payload);
    if (!records.isEmpty()
        && !payloadFitsInCurrentBatch(records.size() * records.get(0).size(), payload.length)) {
      rollback(connectionHandler, payload);
      long mutationCount = this.mutationCount + records.size() * records.get(0).size();
      long commitSize = this.batchSize + payload.length;
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.FAILED_PRECONDITION,
          "Mutation count: "
              + mutationCount
              + " or mutation commit size: "
              + commitSize
              + " has exceeded the limit.");
    }
    for (CSVRecord record : records) {
      // Check that the number of columns in a record matches the number of columns in the table
      if (record.size() != this.tableColumns.keySet().size()) {
        rollback(connectionHandler, payload);
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
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
            case BOOL:
              builder.set(columnName).to(Boolean.parseBoolean(recordValue));
              break;
            case INT64:
              builder.set(columnName).to(Long.parseLong(recordValue));
              break;
            case FLOAT64:
              builder.set(columnName).to(Double.parseDouble(recordValue));
              break;
            case BYTES:
              builder.set(columnName).to(Byte.parseByte(recordValue));
              break;
            case TIMESTAMP:
              builder.set(columnName).to(com.google.cloud.Timestamp.parseTimestamp(recordValue));
              break;
          }
        } catch (NumberFormatException | DateTimeParseException e) {
          rollback(connectionHandler, payload);
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT,
              "Invalid input syntax for type "
                  + columnType.toString()
                  + ":"
                  + "\""
                  + recordValue
                  + "\"");
        } catch (IllegalArgumentException e) {
          rollback(connectionHandler, payload);
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT, "Invalid input syntax for column \"" + columnName + "\"");
        } catch (Exception e) {
          rollback(connectionHandler, payload);
          throw e;
        }
      }
      this.mutations.add(builder.build()); // Add write builder to mutation list
      this.mutationCount += record.size(); // Increment the number of mutations being added
      this.rowCount++; // Increment the number of COPY rows by one
    }
    this.batchSize += payload.length; // Increment the batch size based on payload length
  }

  /**
   * @return True if adding payload to current batch will fit under mutation limit and batch size
   *     limit, false otherwise.
   */
  private boolean payloadFitsInCurrentBatch(int rowMutationCount, int payloadLength) {
    return (this.mutationCount + rowMutationCount <= MUTATION_LIMIT
        && this.batchSize + payloadLength <= COMMIT_LIMIT);
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
    return parser.getRecords();
  }

  /**
   * Write list of mutations to Spanner and reset for next batch
   *
   * @return count of the number of rows updated.
   */
  public int writeToSpanner(ConnectionHandler connectionHandler) {
    if (this.mutations.isEmpty()) {
      return 0;
    }
    Connection connection = connectionHandler.getSpannerConnection();
    connection.write(this.mutations); // Write mutation to spanner
    // Reset mutations, mutation counter, and batch size count for a new batch
    this.mutations = new ArrayList<>();
    this.mutationCount = 0;
    this.batchSize = 0;
    return this.rowCount;
  }

  public void rollback(ConnectionHandler connectionHandler, byte[] payload) throws Exception {
    Connection connection = connectionHandler.getSpannerConnection();
    connection.rollback();
    this.mutations = new ArrayList<>();
    this.mutationCount = 0;
    this.batchSize = 0;
    createErrorFile(payload);
  }

  public void createErrorFile(byte[] payload) throws IOException {
    File unsuccessfulCopy = new File(ERROR_FILE);
    if (unsuccessfulCopy.createNewFile()) {
      this.fileWriter = new FileWriter(ERROR_FILE);
      writeToErrorFile(payload);
    } else {
      System.err.println("File " + unsuccessfulCopy.getName() + " already exists");
    }
  }

  public void writeToErrorFile(byte[] payload) throws IOException {
    if (this.fileWriter != null) {
      this.fileWriter.write(new String(payload, StandardCharsets.UTF_8).trim() + "\n");
    }
  }

  public void writeMutationsToErrorFile() throws IOException {
    File unsuccessfulCopy = new File(ERROR_FILE);
    if (unsuccessfulCopy.createNewFile()) {
      this.fileWriter = new FileWriter(ERROR_FILE);
    }

    for (Mutation mutation : this.mutations) {
      this.fileWriter.write(mutation.toString());
    }
  }

  public void closeErrorFile() throws IOException {
    if (this.fileWriter != null) {
      this.fileWriter.close();
    }
  }
}
