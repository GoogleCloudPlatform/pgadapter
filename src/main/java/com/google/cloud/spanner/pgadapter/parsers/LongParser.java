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
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
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
            this.item =
                new BigDecimal(stringValue).setScale(0, RoundingMode.HALF_UP).longValueExact();
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
    return convertToPG(this.item);
  }

  static byte[] convertToPG(long value) {
    byte[] result = new byte[8];
    ByteConverter.int8(result, 0, value);
    return result;
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return Long.toString(resultSet.getLong(position)).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getLong(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
