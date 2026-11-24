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
import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to long. */
@InternalApi
public class LongParser extends Parser<Long> {

  private static final int MAX_DECIMAL_LENGTH = 128;
  private static final int MAX_INTEGER_DIGITS = 20;

  LongParser(ResultSet item, int position) {
    this.item = item.getLong(position);
  }

  LongParser(Object item) {
    this.item = (Long) item;
  }

  LongParser(byte[] item, FormatCode formatCode) {
    this.item = toLong(item, formatCode);
  }

  /** Converts the given data to a long based on the format code. */
  public static Long toLong(byte[] item, FormatCode formatCode) {
    if (item == null) {
      return null;
    }
    switch (formatCode) {
      case TEXT:
        return parseTextInteger(item, "int8", Long.MIN_VALUE, Long.MAX_VALUE);
      case BINARY:
        return toLong(item);
      default:
        throw new IllegalArgumentException("Unsupported format: " + formatCode);
    }
  }

  static long parseTextInteger(
      @Nonnull byte[] item, @Nonnull String typeName, long minValue, long maxValue) {
    // String.trim() also strips ASCII control characters <= 0x20 (such as \0 or \u0001), which is
    // slightly more permissive than PostgreSQL whitespace, and Long.parseLong/BigDecimal accept
    // non-ASCII Unicode digits. We intentionally rely on the standard library here to avoid a
    // custom parser for these extreme outliers.
    String stringValue = new String(item, UTF8).trim();
    long parsedValue;
    try {
      parsedValue = Long.parseLong(stringValue);
    } catch (NumberFormatException numberFormatException) {
      if (isPlainIntegerSyntax(stringValue)) {
        throw invalidValueException(typeName, stringValue, SQLState.NumericValueOutOfRange);
      }
      parsedValue = parseDecimalToLong(stringValue, typeName);
    }
    if (parsedValue < minValue || parsedValue > maxValue) {
      throw invalidValueException(typeName, stringValue, SQLState.NumericValueOutOfRange);
    }
    return parsedValue;
  }

  private static boolean isPlainIntegerSyntax(String value) {
    if (value.isEmpty()) {
      return false;
    }
    int startIndex = (value.charAt(0) == '+' || value.charAt(0) == '-') ? 1 : 0;
    if (startIndex == value.length()) {
      return false;
    }
    for (int index = startIndex; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character < '0' || character > '9') {
        return false;
      }
    }
    return true;
  }

  private static long parseDecimalToLong(String stringValue, String typeName) {
    // Bound non-integer string length before calling new BigDecimal(...) so multi-MB fractional
    // strings cannot trigger expensive BigInteger base conversion or large allocations.
    // Because decimal.precision() <= stringValue.length() <= MAX_DECIMAL_LENGTH, any non-zero
    // value reaching setScale(0) with 0 <= integerDigits <= MAX_INTEGER_DIGITS is guaranteed to
    // have -19 <= scale <= MAX_DECIMAL_LENGTH, preventing setScale(0) CPU/heap exhaustion.
    // Overly long decimal/scientific strings (e.g., > 128 chars) are intentionally rejected.
    if (stringValue.length() > MAX_DECIMAL_LENGTH) {
      throw invalidValueException(typeName, stringValue, SQLState.SyntaxError);
    }
    BigDecimal decimal;
    try {
      // BigDecimal only supports 32-bit int exponents, so extreme exponents outside int range
      // (e.g., 1e999999999999 or 1e-2147483648) intentionally fail here with SyntaxError.
      decimal = new BigDecimal(stringValue);
    } catch (NumberFormatException exception) {
      throw invalidValueException(typeName, stringValue, SQLState.SyntaxError);
    }
    if (decimal.signum() == 0) {
      return 0L;
    }
    long integerDigits = (long) decimal.precision() - (long) decimal.scale();
    if (integerDigits < 0) {
      // |decimal| < 0.1, so HALF_UP rounding to an integer always produces 0 without materializing
      // 10^scale.
      return 0L;
    }
    if (integerDigits > MAX_INTEGER_DIGITS) {
      // |decimal| >= 10^20 > Long.MAX_VALUE, so it cannot fit in int8.
      throw invalidValueException(typeName, stringValue, SQLState.NumericValueOutOfRange);
    }
    try {
      return decimal.setScale(0, RoundingMode.HALF_UP).longValueExact();
    } catch (ArithmeticException exception) {
      throw invalidValueException(typeName, stringValue, SQLState.NumericValueOutOfRange);
    }
  }

  private static PGException invalidValueException(
      String typeName, String stringValue, SQLState sqlState) {
    return PGExceptionFactory.newPGException(
        "Invalid " + typeName + " value: " + stringValue, sqlState);
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

  public static byte[] convertToPG(
      SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    writeToPG(sessionState, outputStream, resultSet, position, format);
    return null;
  }

  static void writeToPG(
      SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(sessionState, outputStream, getLongAsString(resultSet, position));
        break;
      case POSTGRESQL_BINARY:
        outputStream.writeInt(8);
        outputStream.writeLong(resultSet.getLong(position));
        break;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  static String getLongAsString(ResultSet resultSet, int column) {
    if (resultSet instanceof ProtobufResultSet
        && ((ProtobufResultSet) resultSet).canGetProtobufValue(column)) {
      return ((ProtobufResultSet) resultSet).getProtobufValue(column).getStringValue();
    }
    return Long.toString(resultSet.getLong(column));
  }

  public static void bind(
      ImmutableMap.Builder<String, Value> parametersBuilder,
      String name,
      byte[] item,
      FormatCode formatCode) {
    parametersBuilder.put(name, Value.int64(toLong(item, formatCode)));
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    parametersBuilder.put(name, Value.int64(this.item));
  }
}
