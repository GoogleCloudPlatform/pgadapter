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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to float. */
public class FloatParser extends Parser<Float> {

  FloatParser(ResultSet item, int position) {
    this.item = item.getFloat(position);
  }

  FloatParser(Object item) {
    this.item = (Float) item;
  }

  FloatParser(byte[] item, FormatCode formatCode) {
    this.item = toFloat(item, formatCode);
  }

  /** Converts the given data to a float based on the format code. */
  public static Float toFloat(byte[] item, FormatCode formatCode) {
    if (item == null) {
      return null;
    }
    switch (formatCode) {
      case TEXT:
        String stringValue = new String(item);
        return parseFloat(stringValue);
      case BINARY:
        if (item.length < 4) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.INVALID_ARGUMENT, "Invalid length for float4: " + item.length);
        }
        return ByteConverter.float4(item, 0);
      default:
        throw new IllegalArgumentException("Unsupported format: " + formatCode);
    }
  }

  public static float toFloat(@Nonnull byte[] data) {
    if (data.length < 4) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid length for float4: " + data.length);
    }
    return ByteConverter.float4(data, 0);
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : Float.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return convertToPG(this.item);
  }

  static byte[] convertToPG(float value) {
    byte[] result = new byte[4];
    ByteConverter.float4(result, 0, value);
    return result;
  }

  public static byte[] convertToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    writeToPG(sessionState, outputStream, resultSet, position, format);
    return null;
  }

  static void writeToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(
            sessionState, outputStream, Float.toString(resultSet.getFloat(position)));
        break;
      case POSTGRESQL_BINARY:
        outputStream.writeInt(4);
        outputStream.writeFloat(resultSet.getFloat(position));
        break;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  public static void bind(
      ImmutableMap.Builder<String, Value> parametersBuilder,
      String name,
      byte[] item,
      FormatCode formatCode) {
    parametersBuilder.put(name, toValue(toFloat(item, formatCode)));
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    parametersBuilder.put(name, toValue(this.item));
  }

  /**
   * Converts a string to a float. This method correctly recognizes valid PostgreSQL formats for the
   * special Inf, -Inf, and NaN.
   */
  public static float parseFloat(@Nonnull String s) throws PGException {
    if (Strings.isNullOrEmpty(s)) {
      throw PGExceptionFactory.newPGException("Invalid float4 value: " + s, SQLState.SyntaxError);
    }
    // Quick check for Inf, -Inf, and NaN.
    s = s.trim();
    if (s.isEmpty()) {
      throw PGExceptionFactory.newPGException("Invalid float4 value: " + s, SQLState.SyntaxError);
    }
    if (s.equalsIgnoreCase("inf")
        || s.equalsIgnoreCase("infinity")
        || s.equalsIgnoreCase("+inf")
        || s.equalsIgnoreCase("+infinity")) {
      return Float.POSITIVE_INFINITY;
    }
    if (s.equalsIgnoreCase("-inf") || s.equalsIgnoreCase("-infinity")) {
      return Float.NEGATIVE_INFINITY;
    }
    if (s.equalsIgnoreCase("NaN")) {
      return Float.NaN;
    }
    try {
      return Float.parseFloat(s);
    } catch (NumberFormatException exception) {
      throw PGExceptionFactory.newPGException("Invalid float4 value: " + s, SQLState.SyntaxError);
    }
  }

  /**
   * Converts a float to a Spanner {@link Value}. This method correctly encodes the special values
   * Inf, -Inf, and NaN into a string value.
   */
  public static Value toValue(Float value) {
    if (value != null) {
      if (value == Float.POSITIVE_INFINITY) {
        return Value.string("Infinity");
      } else if (value == Float.NEGATIVE_INFINITY) {
        return Value.string("-Infinity");
      } else if (Float.isNaN(value)) {
        return Value.string("NaN");
      }
    }
    return Value.float32(value);
  }
}
