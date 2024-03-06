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
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/**
 * Parse specified data to boolean. For most cases it is simply translating from chars 't'/'f' to
 * bit, or simply returning the bit representation.
 */
@InternalApi
public class BooleanParser extends Parser<Boolean> {
  private static final String TRUE_VALUE = "t";
  private static final String FALSE_VALUE = "f";
  private static final byte[] TRUE_VALUE_BYTES = new byte[] {'t'};
  private static final byte[] FALSE_VALUE_BYTES = new byte[] {'f'};
  private static final byte[] TRUE_VALUE_BYTES_BINARY = new byte[1];
  private static final byte[] FALSE_VALUE_BYTES_BINARY = new byte[1];

  static {
    ByteConverter.bool(TRUE_VALUE_BYTES_BINARY, 0, true);
    ByteConverter.bool(FALSE_VALUE_BYTES_BINARY, 0, false);
  }
  // See https://www.postgresql.org/docs/current/datatype-boolean.html
  private static final Set<String> TRUE_VALUES =
      ImmutableSet.of("t", "tr", "tru", "true", "y", "ye", "yes", "on", "1");
  private static final Set<String> FALSE_VALUES =
      ImmutableSet.of("f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0");

  BooleanParser(ResultSet item, int position) {
    this.item = item.getBoolean(position);
  }

  BooleanParser(Object item) {
    this.item = (Boolean) item;
  }

  BooleanParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item, UTF8).toLowerCase(Locale.ENGLISH);
          this.item = toBoolean(stringValue);
          break;
        case BINARY:
          this.item = toBoolean(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the given binary data to a boolean value. */
  public static boolean toBoolean(@Nonnull byte[] data) {
    if (data.length == 0) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for bool: " + data.length);
    }
    return ByteConverter.bool(data, 0);
  }

  /** Converts the string to a boolean value according to the PostgreSQL specs. */
  public static boolean toBoolean(String value) {
    value = value == null ? null : value.toLowerCase(Locale.ENGLISH);
    if (TRUE_VALUES.contains(value)) {
      return true;
    } else if (FALSE_VALUES.contains(value)) {
      return false;
    } else {
      throw PGExceptionFactory.newPGException(value + " is not a valid boolean value");
    }
  }

  @Override
  public String stringParse() {
    if (this.item == null) {
      return null;
    }
    return this.item ? TRUE_VALUE : FALSE_VALUE;
  }

  @Override
  protected String spannerParse() {
    return this.item == null ? null : Boolean.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPGBinary(this.item);
  }

  static byte[] convertToPGBinary(boolean value) {
    byte[] result = new byte[1];
    ByteConverter.bool(result, 0, value);
    return result;
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
        return Boolean.toString(resultSet.getBoolean(position)).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_TEXT:
        return resultSet.getBoolean(position) ? TRUE_VALUE_BYTES : FALSE_VALUE_BYTES;
      case POSTGRESQL_BINARY:
        return resultSet.getBoolean(position) ? TRUE_VALUE_BYTES_BINARY : FALSE_VALUE_BYTES_BINARY;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
