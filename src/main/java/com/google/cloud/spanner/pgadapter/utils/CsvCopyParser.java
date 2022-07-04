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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.common.collect.Iterators;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.postgresql.jdbc.TimestampUtils;

class CsvCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(CsvCopyParser.class.getName());

  private final TimestampUtils timestampUtils = createDefaultTimestampUtils();
  private final CSVFormat format;
  private final PipedOutputStream payload;
  private final int pipeBufferSize;
  private boolean hasHeader;
  private boolean isHeaderParsed;
  private final CSVParser parser;

  static TimestampUtils createDefaultTimestampUtils() {
    return new TimestampUtils(false, () -> null);
  }

  CsvCopyParser(
      CSVFormat csvFormat, PipedOutputStream payload, int pipeBufferSize, boolean hasHeader)
      throws IOException {
    this.format = csvFormat;
    this.payload = payload;
    this.pipeBufferSize = pipeBufferSize;
    this.hasHeader = hasHeader;
    this.parser = createParser();
  }

  @Override
  public Iterator<CopyRecord> iterator() {
    return Iterators.transform(
        parser.iterator(), record -> new CsvCopyRecord(this.timestampUtils, record));
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

  CSVParser createParser() throws IOException {
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

  static class CsvCopyRecord implements CopyRecord {
    private final TimestampUtils timestampUtils;
    private final CSVRecord record;

    CsvCopyRecord(TimestampUtils timestampUtils, CSVRecord record) {
      this.timestampUtils = timestampUtils;
      this.record = record;
    }

    @Override
    public int numColumns() {
      return record.size();
    }

    @Override
    public Value getValue(TypeCode typeCode, String columnName) throws SpannerException {
      String recordValue = "";
      try {
        recordValue = record.get(columnName);
        switch (typeCode) {
          case STRING:
            return Value.string(recordValue);
          case JSON:
            return Value.json(recordValue);
          case BOOL:
            return Value.bool(recordValue == null ? null : Boolean.parseBoolean(recordValue));
          case INT64:
            return Value.int64(recordValue == null ? null : Long.parseLong(recordValue));
          case FLOAT64:
            return Value.float64(recordValue == null ? null : Double.parseDouble(recordValue));
          case NUMERIC:
            return Value.pgNumeric(recordValue);
          case BYTES:
            if (recordValue == null) {
              return Value.bytes(null);
            } else if (recordValue.startsWith("\\x")) {
              return Value.bytes(ByteArray.copyFrom(Hex.decodeHex(recordValue.substring(2))));
            } else {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.INVALID_ARGUMENT,
                  "COPY only supports the Hex format for bytea columns");
            }
          case DATE:
            return Value.date(recordValue == null ? null : Date.parseDate(recordValue));
          case TIMESTAMP:
            Timestamp timestamp = timestampUtils.toTimestamp(null, recordValue);
            return Value.timestamp(
                timestamp == null ? null : com.google.cloud.Timestamp.of(timestamp));
          default:
            SpannerException spannerException =
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.INVALID_ARGUMENT, "Unknown or unsupported type: " + typeCode);
            logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
            throw spannerException;
        }
      } catch (NumberFormatException | DateTimeParseException e) {
        SpannerException spannerException =
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Invalid input syntax for type "
                    + typeCode.toString()
                    + ":"
                    + "\""
                    + recordValue
                    + "\"",
                e);
        logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
        throw spannerException;
      } catch (IllegalArgumentException e) {
        SpannerException spannerException =
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Invalid input syntax for column \"" + columnName + "\"",
                e);
        logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
        throw spannerException;
      } catch (Exception e) {
        SpannerException spannerException = SpannerExceptionFactory.asSpannerException(e);
        logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
        throw spannerException;
      }
    }
  }
}
