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
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;
import com.google.spanner.v1.TypeCode;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import org.postgresql.core.Oid;
import org.postgresql.jdbc.TimestampUtils;

class BinaryCopyParser implements CopyInParser {
  private static final Logger logger = Logger.getLogger(BinaryCopyParser.class.getName());
  private static final char[] EXPECTED_HEADER =
      new char[] {'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\0'};

  private final TimestampUtils timestampUtils = CopyInParser.createDefaultTimestampUtils();
  private final DataInputStream dataInputStream;
  private final boolean containsOids;
  private boolean calledIterator = false;
  private short firstRowFieldCount = -1;

  BinaryCopyParser(PipedOutputStream payload, int pipeBufferSize) throws IOException {
    this.dataInputStream = new DataInputStream(new PipedInputStream(payload, pipeBufferSize));
    verifyBinaryHeader();
    int flags = this.dataInputStream.readInt();
    this.containsOids = ((flags & (1L << 16)) != 0);
    // This should according to the current spec always be zero.
    // But if it happens not to be so, we should just skip the following bytes.
    int headerExtensionLength = this.dataInputStream.readInt();
    while (headerExtensionLength > 0) {
      headerExtensionLength -= this.dataInputStream.skip(headerExtensionLength);
    }
  }

  @Override
  public Iterator<CopyRecord> iterator() {
    Preconditions.checkState(!this.calledIterator, "Can only call iterator() once");
    this.calledIterator = true;
    return new BinaryIterator();
  }

  @Override
  public void close() throws IOException {}

  void verifyBinaryHeader() throws IOException {
    // Binary COPY files should have the following 11-bytes fixed header:
    // PGCOPY\n\377\r\n\0
    char[] header = new char[11];
    for (int i = 0; i < header.length; i++) {
      header[i] = this.dataInputStream.readChar();
    }
    if (!Arrays.equals(EXPECTED_HEADER, header)) {
      throw new IOException(
          String.format(
              "Invalid COPY header encountered.\nGot:  %s\nWant: %s",
              String.valueOf(header), String.valueOf(EXPECTED_HEADER)));
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
      } catch (IOException ioException) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INTERNAL, ioException.getMessage(), ioException);
      }
    }

    @Override
    public CopyRecord next() {
      try {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
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
    public Value getValue(TypeCode typeCode, String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Value getValue(TypeCode typeCode, int columnIndex) {
      Preconditions.checkArgument(
          columnIndex >= 0 && columnIndex < numColumns(),
          "columnIndex must be >= 0 && < numColumns");
      BinaryField field = fields[columnIndex];
      return null;
    }
  }
}
