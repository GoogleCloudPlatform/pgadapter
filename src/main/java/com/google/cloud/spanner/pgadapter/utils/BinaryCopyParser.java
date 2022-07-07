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
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.parsers.BinaryParser;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.DateParser;
import com.google.cloud.spanner.pgadapter.parsers.DoubleParser;
import com.google.cloud.spanner.pgadapter.parsers.LongParser;
import com.google.cloud.spanner.pgadapter.parsers.NumericParser;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.cloud.spanner.pgadapter.parsers.StringParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.TimestampUtils;

class BinaryCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(BinaryCopyParser.class.getName());
  private static final byte[] EXPECTED_HEADER =
      new byte[] {'P', 'G', 'C', 'O', 'P', 'Y', '\n', -1, '\r', '\n', '\0'};

  private final TimestampUtils timestampUtils = CopyInParser.createDefaultTimestampUtils();
  private final DataInputStream dataInputStream;
  private boolean containsOids;
  private boolean calledIterator = false;
  private short firstRowFieldCount = -1;

  BinaryCopyParser(PipedOutputStream payload, int pipeBufferSize) throws IOException {
    this.dataInputStream = new DataInputStream(new PipedInputStream(payload, pipeBufferSize));
  }

  @Override
  public Iterator<CopyRecord> iterator() {
    Preconditions.checkState(!this.calledIterator, "Can only call iterator() once");
    this.calledIterator = true;

    try {
      verifyBinaryHeader();
      System.out.println("Binary header verified");
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
    if (!Arrays.equals(EXPECTED_HEADER, header)) {
      throw new IOException(
          String.format(
              "Invalid COPY header encountered.\nGot:  %s\nWant: %s",
              new String(header, StandardCharsets.UTF_8),
              new String(EXPECTED_HEADER, StandardCharsets.UTF_8)));
    }
  }

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
        if (hasNext == HasNext.UNKNOWN) {
          short fieldCount = dataInputStream.readShort();
          if (fieldCount == -1) {
            logger.log(Level.INFO, "End of copy file: -1");
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
        logger.log(Level.INFO, "EOF in BinaryCopyParser");
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
        if (!hasNext()) {
          logger.log(
              Level.WARNING, "tried to call next() on BinaryCopyParser with no more elements");
          throw new NoSuchElementException();
        }
        hasNext = HasNext.UNKNOWN;
        int oid = Oid.UNSPECIFIED;
        if (containsOids) {
          int length = dataInputStream.readInt();
          if (length != 4) {
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.FAILED_PRECONDITION, "Invalid length for OID: " + length);
          }
          oid = dataInputStream.readInt();
        }
        for (int field = 0; field < firstRowFieldCount; field++) {
          int length = dataInputStream.readInt();
          if (length == -1) {
            currentRow[field] = new BinaryField(null, oid);
          } else if (length > -1) {
            byte[] data = new byte[length];
            dataInputStream.readFully(data);
            currentRow[field] = new BinaryField(data, oid);
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
    private final int oid;

    BinaryField(byte[] data, int oid) {
      this.data = data;
      this.oid = oid;
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
      throw new UnsupportedOperationException();
    }

    @Override
    public Value getValue(Type type, int columnIndex) {
      Preconditions.checkArgument(
          columnIndex >= 0 && columnIndex < numColumns(),
          "columnIndex must be >= 0 && < numColumns");
      BinaryField field = fields[columnIndex];
      if (field.oid != Oid.UNSPECIFIED) {
        int columnOid = Parser.toOid(type);
        if (columnOid != field.oid) {
          String message =
              String.format(
                  "Conversion of binary values from %d to %d is not supported",
                  field.oid, columnOid);
          logger.log(Level.WARNING, message);
          throw SpannerExceptionFactory.newSpannerException(ErrorCode.FAILED_PRECONDITION, message);
        }
      }
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
        case JSON:
          return Value.json(field.data == null ? null : StringParser.toString(field.data));
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
