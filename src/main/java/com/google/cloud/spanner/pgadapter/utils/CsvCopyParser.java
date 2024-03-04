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
import com.google.cloud.spanner.pgadapter.parsers.ArrayParser;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.postgresql.core.Oid;

/** Implementation of {@link CopyInParser} for the TEXT and CSV formats. */
class CsvCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(CsvCopyParser.class.getName());

  private final SessionState sessionState;
  private final CSVFormat format;
  private final boolean hasHeader;
  private final CSVParser parser;

  CsvCopyParser(
      SessionState sessionState,
      CSVFormat csvFormat,
      PipedInputStream inputStream,
      boolean hasHeader)
      throws IOException {
    this.sessionState = sessionState;
    this.format = csvFormat;
    this.hasHeader = hasHeader;
    this.parser = createParser(inputStream);
  }

  @Override
  public Iterator<CopyRecord> iterator() {
    return Iterators.transform(
        parser.iterator(), record -> new CsvCopyRecord(this.sessionState, record, this.hasHeader));
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

  CSVParser createParser(PipedInputStream inputStream) throws IOException {
    // Construct the CSVParser directly on the stream of incoming CopyData messages, so we don't
    // store more data in memory than necessary. Loading all data into memory first before starting
    // to parse and write the CSVRecords could otherwise cause an out-of-memory exception for large
    // files.
    Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    return CSVParser.parse(reader, this.format);
  }

  static class CsvCopyRecord implements CopyRecord {
    private final SessionState sessionState;
    private final CSVRecord record;
    private final boolean hasHeader;

    CsvCopyRecord(SessionState sessionState, CSVRecord record, boolean hasHeader) {
      this.sessionState = sessionState;
      this.record = record;
      this.hasHeader = hasHeader;
    }

    @Override
    public int numColumns() {
      return record.size();
    }

    @Override
    public boolean isEndRecord() {
      // End of data can be represented by a single line containing just backslash-period (\.). An
      // end-of-data marker is not necessary when reading from a file, since the end of file serves
      // perfectly well; it is needed only when copying data to or from client applications using
      // pre-3.0 client protocol.
      return record.size() == 1 && Objects.equals("\\.", record.get(0));
    }

    @Override
    public boolean hasColumnNames() {
      return this.hasHeader;
    }

    @Override
    public boolean isNull(int columnIndex) {
      return record.get(columnIndex) == null;
    }

    @Override
    public Value getValue(Type type, String columnName) throws SpannerException {
      String recordValue = record.get(columnName);
      return getSpannerValue(sessionState, type, recordValue);
    }

    @Override
    public Value getValue(Type type, int columnIndex) throws SpannerException {
      String recordValue = record.get(columnIndex);
      return getSpannerValue(sessionState, type, recordValue);
    }

    static Value getSpannerValue(SessionState sessionState, Type type, String recordValue)
        throws SpannerException {
      try {
        switch (type.getCode()) {
          case STRING:
            return Value.string(recordValue);
          case PG_JSONB:
            return Value.pgJsonb(recordValue);
          case BOOL:
            return Value.bool(recordValue == null ? null : BooleanParser.toBoolean(recordValue));
          case INT64:
            return Value.int64(recordValue == null ? null : Long.parseLong(recordValue));
          case FLOAT32:
            return Value.float32(recordValue == null ? null : Float.parseFloat(recordValue));
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
                recordValue == null
                    ? null
                    : TimestampParser.toTimestamp(recordValue, sessionState.getTimezone());
            return Value.timestamp(timestamp);
          case ARRAY:
            switch (type.getArrayElementType().getCode()) {
              case STRING:
                return Value.stringArray(
                    cast(ArrayParser.stringArrayToList(recordValue, Oid.TEXT, sessionState, true)));
              case PG_JSONB:
                return Value.pgJsonbArray(
                    cast(
                        ArrayParser.stringArrayToList(recordValue, Oid.JSONB, sessionState, true)));
              case BOOL:
                return Value.boolArray(
                    cast(ArrayParser.stringArrayToList(recordValue, Oid.BOOL, sessionState, true)));
              case INT64:
                return Value.int64Array(
                    cast(ArrayParser.stringArrayToList(recordValue, Oid.INT8, sessionState, true)));
              case FLOAT32:
                return Value.float32Array(
                    cast(
                        ArrayParser.stringArrayToList(
                            recordValue, Oid.FLOAT4, sessionState, true)));
              case FLOAT64:
                return Value.float64Array(
                    cast(
                        ArrayParser.stringArrayToList(
                            recordValue, Oid.FLOAT8, sessionState, true)));
              case PG_NUMERIC:
                return Value.pgNumericArray(
                    cast(
                        ArrayParser.stringArrayToList(
                            recordValue, Oid.NUMERIC, sessionState, true)));
              case BYTES:
                return Value.bytesArray(
                    cast(
                        ArrayParser.stringArrayToList(recordValue, Oid.BYTEA, sessionState, true)));
              case DATE:
                return Value.dateArray(
                    cast(ArrayParser.stringArrayToList(recordValue, Oid.DATE, sessionState, true)));
              case TIMESTAMP:
                return Value.timestampArray(
                    cast(
                        ArrayParser.stringArrayToList(
                            recordValue, Oid.TIMESTAMPTZ, sessionState, true)));
            }
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

  @SuppressWarnings("unchecked")
  private static <T> List<T> cast(List<?> list) {
    return (List<T>) list;
  }
}
