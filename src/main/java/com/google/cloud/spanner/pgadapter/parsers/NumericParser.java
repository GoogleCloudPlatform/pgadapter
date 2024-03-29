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

package com.google.cloud.spanner.pgadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to {@link Number}. */
@InternalApi
public class NumericParser extends Parser<String> {
  private static final byte[] NAN = new byte[8];

  static {
    ByteConverter.int2(NAN, 4, (short) 0xC000);
  }

  NumericParser(ResultSet item, int position) {
    this.item = item.isNull(position) ? null : item.getString(position);
  }

  NumericParser(Object item) {
    this.item = item == null ? null : item.toString();
  }

  NumericParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = new String(item);
          break;
        case BINARY:
          this.item = toNumericString(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }
  /**
   * Converts the binary data to a string representation of the numeric value. That is either a
   * valid numeric string or 'NaN'.
   */
  public static String toNumericString(@Nonnull byte[] data) {
    if (data.length < 8) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for numeric: " + data.length);
    }
    Number number = ByteConverter.numeric(data, 0, data.length);
    return number == null ? null : number.toString();
  }

  @Override
  public String stringParse() {
    return this.item;
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPG(this.item);
  }

  static byte[] convertToPG(@Nonnull String value) {
    if (value.equalsIgnoreCase("NaN")) {
      return NAN;
    }
    try {
      return ByteConverter.numeric(new BigDecimal(value));
    } catch (NumberFormatException exception) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid numeric value: " + value);
    }
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return resultSet.getString(position).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getString(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(Value.pgNumeric(stringParse()));
  }
}
