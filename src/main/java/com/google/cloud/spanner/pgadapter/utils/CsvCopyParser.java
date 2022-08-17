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
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Implementation of {@link CopyInParser} for the TEXT and CSV formats. */
class CsvCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(CsvCopyParser.class.getName());

  private final CSVFormat format;
  private final PipedOutputStream payload;
  private final int pipeBufferSize;
  private final boolean hasHeader;
  private final CSVParser parser;

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
        parser.iterator(), record -> new CsvCopyRecord(record, this.hasHeader));
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
    if (this.hasHeader) {
      parser = CSVParser.parse(reader, this.format.withFirstRecordAsHeader());
    } else {
      parser = CSVParser.parse(reader, this.format);
    }
    return parser;
  }

  static class CsvCopyRecord implements CopyRecord {
    private final CSVRecord record;
    private final boolean hasHeader;

    CsvCopyRecord(CSVRecord record, boolean hasHeader) {
      this.record = record;
      this.hasHeader = hasHeader;
    }

    @Override
    public int numColumns() {
      return record.size();
    }

    @Override
    public boolean hasColumnNames() {
      return this.hasHeader;
    }

    @Override
    public Value getValue(Type type, String columnName) throws SpannerException {
      String recordValue = record.get(columnName);
      return getSpannerValue(type, recordValue);
    }

    @Override
    public Value getValue(Type type, int columnIndex) throws SpannerException {
      String recordValue = record.get(columnIndex);
      return getSpannerValue(type, recordValue);
    }

    static Value getSpannerValue(Type type, String recordValue) throws SpannerException {
      try {
        switch (type.getCode()) {
          case STRING:
            return Value.string(recordValue);
          case JSON:
            return Value.json(recordValue);
          case BOOL:
            return Value.bool(recordValue == null ? null : BooleanParser.toBoolean(recordValue));
          case INT64:
            return Value.int64(recordValue == null ? null : Long.parseLong(recordValue));
          case FLOAT64:
            return Value.float64(recordValue == null ? null : Double.parseDouble(recordValue));
          case PG_NUMERIC:
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
            Timestamp timestamp =
                recordValue == null ? null : TimestampParser.toTimestamp(recordValue);
            return Value.timestamp(timestamp);
          default:
            SpannerException spannerException =
                SpannerExceptionFactory.newSpannerException(
                    ErrorCode.INVALID_ARGUMENT, "Unknown or unsupported type: " + type);
            logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
            throw spannerException;
        }
      } catch (NumberFormatException | DateTimeParseException e) {
        SpannerException spannerException =
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Invalid input syntax for type " + type + ":" + "\"" + recordValue + "\"",
                e);
        logger.log(Level.SEVERE, spannerException.getMessage(), spannerException);
        throw spannerException;
      } catch (IllegalArgumentException e) {
        SpannerException spannerException =
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT, "Invalid input syntax", e);
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
