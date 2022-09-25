// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.pgadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to long. */
@InternalApi
public class LongParser extends Parser<Long> {

  LongParser(ResultSet item, int position) {
    this.item = item.getLong(position);
  }

  LongParser(Object item) {
    this.item = (Long) item;
  }

  LongParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item);
          try {
            this.item = Long.valueOf(stringValue);
          } catch (Exception exception) {
            throw PGExceptionFactory.newPGException("Invalid int8 value: " + stringValue);
          }
          break;
        case BINARY:
          this.item = toLong(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the binary data to a long value. */
  public static long toLong(@Nonnull byte[] data) {
    if (data.length >= 8) {
      return ByteConverter.int8(data, 0);
    } else if (data.length == 4) {
      // We allow 4-byte values for bigint as well, because Spangres allows the use of the int type
      // in create table statements. This is automatically converted to a bigint column, but it
      // could be that someone uses the same DDL statement to create a table in both real
      // PostgresSQL and Spangres, and this keeps copying data between them possible.
      return ByteConverter.int4(data, 0);
    } else {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for int8: " + data.length);
    }
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : Long.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    byte[] result = new byte[8];
    ByteConverter.int8(result, 0, this.item);
    return result;
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
