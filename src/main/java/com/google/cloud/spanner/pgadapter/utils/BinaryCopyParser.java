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

import static com.google.cloud.spanner.pgadapter.statements.CopyToStatement.COPY_BINARY_HEADER;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.parsers.BinaryParser;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.DateParser;
import com.google.cloud.spanner.pgadapter.parsers.DoubleParser;
import com.google.cloud.spanner.pgadapter.parsers.JsonbParser;
import com.google.cloud.spanner.pgadapter.parsers.LongParser;
import com.google.cloud.spanner.pgadapter.parsers.NumericParser;
import com.google.cloud.spanner.pgadapter.parsers.StringParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class parses a stream of copy data in the PostgreSQL binary format
 * (https://www.postgresql.org/docs/current/sql-copy.html) and converts them to a stream of {@link
 * CopyRecord}s.
 */
class BinaryCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(BinaryCopyParser.class.getName());

  private final DataInputStream dataInputStream;
  private boolean containsOids;
  private boolean calledIterator = false;
  private short firstRowFieldCount = -1;

  BinaryCopyParser(PipedInputStream inputStream) {
    this.dataInputStream = new DataInputStream(new BufferedInputStream(inputStream));
  }

  @Override
  public Iterator<CopyRecord> iterator() {
    Preconditions.checkState(!this.calledIterator, "Can only call iterator() once");
    this.calledIterator = true;

    try {
      // The PostgreSQL binary copy format uses a fixed header to indicate that it is a file/stream
      // in binary format.
      verifyBinaryHeader();
      // The fixed header is followed by two ints:
      // 1. flags: Contains specific flags for the binary copy format.
      // 2. header extension length: Currently zero, but if anything else is encountered, this
      // parser just skips it. This is according to spec, as a reader should skip any extension it
      // does not know.
      int flags = this.dataInputStream.readInt();
      this.containsOids = ((flags & (1L << 16)) != 0);
      // This should according to the current spec always be zero.
      // But if it happens not to be so, we should just skip the following bytes.
      int headerExtensionLength = this.dataInputStream.readInt();
      while (headerExtensionLength > 0) {
        headerExtensionLength -= this.dataInputStream.skip(headerExtensionLength);
      }
    } catch (IOException ioException) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INTERNAL, "Failed to read binary file header", ioException);
    }

    return new BinaryIterator();
  }

  @Override
  public void close() throws IOException {}

  void verifyBinaryHeader() throws IOException {
    // Binary COPY files should have the following 11-bytes fixed header:
    // PGCOPY\n\377\r\n\0
    byte[] header = new byte[11];
    this.dataInputStream.readFully(header);
    if (!Arrays.equals(COPY_BINARY_HEADER, header)) {
      throw new IOException(
          String.format(
              "Invalid COPY header encountered.\nGot:  %s\nWant: %s",
              new String(header, StandardCharsets.UTF_8),
              new String(COPY_BINARY_HEADER, StandardCharsets.UTF_8)));
    }
  }

  /** Tri-value enum for the hasNext() call. */
  enum HasNext {
    UNKNOWN,
    YES,
    NO,
  }

  class BinaryIterator implements Iterator<CopyRecord> {
    private BinaryField[] currentRow;
    private HasNext hasNext = HasNext.UNKNOWN;

    @Override
    public boolean hasNext() {
      try {
        // The hasNext status is UNKNOWN if a call to next() has been executed since the last time
        // hasNext() was called, or if this is the first time hasNext() is called.
        if (hasNext == HasNext.UNKNOWN) {
          // The first value in a row is the number of fields in that row. The value will be -1 for
          // the last tuple (this is the file trailer). The value should be the same for all other
          // rows.
          short fieldCount = dataInputStream.readShort();
          if (fieldCount == -1) {
            logger.log(Level.FINE, "End of copy file: -1");
            hasNext = HasNext.NO;
          } else if (fieldCount > -1) {
            if (firstRowFieldCount == -1) {
              firstRowFieldCount = fieldCount;
              currentRow = new BinaryField[fieldCount];
            } else if (firstRowFieldCount != fieldCount) {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION,
                  String.format(
                      "Invalid field count encountered: %d, expected %d",
                      fieldCount, firstRowFieldCount));
            }
            hasNext = HasNext.YES;
          } else {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION,
                String.format("Invalid field count encountered: %d", fieldCount));
          }
        }
        return hasNext == HasNext.YES;
      } catch (EOFException eofException) {
        // The protocol specifies that the stream should contain a -1 as the trailer in the file,
        // but it seems that some clients do not include this.
        logger.log(Level.FINE, "EOF in BinaryCopyParser");
        hasNext = HasNext.NO;
        return false;
      } catch (IOException ioException) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, ioException.getMessage(), ioException);
      }
    }

    @Override
    public CopyRecord next() {
      try {
        // Verify that there is actually a next record.
        if (!hasNext()) {
          logger.log(
              Level.WARNING, "tried to call next() on BinaryCopyParser with no more elements");
          throw new NoSuchElementException();
        }
        // Reset the hasNext status.
        hasNext = HasNext.UNKNOWN;
        // The header flags could indicate that the file contains oids. If so, we just read and
        // ignore them.
        if (containsOids) {
          int length = dataInputStream.readInt();
          if (length != 4) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION, "Invalid length for OID: " + length);
          }
          // Read and ignore the oid.
          dataInputStream.readInt();
        }
        // Each row consists of:
        // 1. The number of fields (this should be the same for all rows)
        // 2. For each field:
        // 2.1. The length of the field.
        // 2.2. The actual data of that field.
        for (int field = 0; field < firstRowFieldCount; field++) {
          int length = dataInputStream.readInt();
          if (length == -1) {
            currentRow[field] = new BinaryField(null);
          } else if (length > -1) {
            byte[] data = new byte[length];
            dataInputStream.readFully(data);
            currentRow[field] = new BinaryField(data);
          } else {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION, "Invalid field length: " + length);
          }
        }
        return new BinaryRecord(currentRow);
      } catch (IOException ioException) {
        logger.log(Level.WARNING, "Failed to read binary COPY record", ioException);
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, ioException.getMessage(), ioException);
      }
    }
  }

  static class BinaryField {
    private final byte[] data;

    BinaryField(byte[] data) {
      this.data = data;
    }
  }

  static class BinaryRecord implements CopyRecord {
    private final BinaryField[] fields;

    BinaryRecord(BinaryField[] fields) {
      this.fields = fields;
    }

    @Override
    public int numColumns() {
      return fields.length;
    }

    @Override
    public boolean hasColumnNames() {
      return false;
    }

    @Override
    public Value getValue(Type type, String columnName) {
      // The binary copy format does not include any column name headers or any type information.
      throw new UnsupportedOperationException();
    }

    @Override
    public Value getValue(Type type, int columnIndex) {
      Preconditions.checkArgument(
          columnIndex >= 0 && columnIndex < numColumns(),
          "columnIndex must be >= 0 && < numColumns");
      BinaryField field = fields[columnIndex];
      switch (type.getCode()) {
        case BOOL:
          return Value.bool(field.data == null ? null : BooleanParser.toBoolean(field.data));
        case INT64:
          return Value.int64(field.data == null ? null : LongParser.toLong(field.data));
        case PG_NUMERIC:
          return Value.pgNumeric(
              field.data == null ? null : NumericParser.toNumericString(field.data));
        case FLOAT64:
          return Value.float64(field.data == null ? null : DoubleParser.toDouble(field.data));
        case STRING:
          return Value.string(field.data == null ? null : StringParser.toString(field.data));
        case PG_JSONB:
          return Value.pgJsonb(field.data == null ? null : JsonbParser.toString(field.data));
        case BYTES:
          return Value.bytes(field.data == null ? null : BinaryParser.toByteArray(field.data));
        case TIMESTAMP:
          return Value.timestamp(
              field.data == null ? null : TimestampParser.toTimestamp(field.data));
        case DATE:
          return Value.date(field.data == null ? null : DateParser.toDate(field.data));
        case ARRAY:
        case STRUCT:
        case NUMERIC:
        default:
          String message = "Unsupported type for COPY: " + type;
          logger.log(Level.WARNING, message);
          throw SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, message);
      }
    }
  }
}
