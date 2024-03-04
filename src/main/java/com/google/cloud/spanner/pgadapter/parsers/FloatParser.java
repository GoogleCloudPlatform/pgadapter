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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import java.nio.charset.StandardCharsets;
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
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item);
          try {
            this.item = Float.valueOf(stringValue);
          } catch (Exception exception) {
            throw PGExceptionFactory.newPGException("Invalid float4 value: " + stringValue);
          }
          break;
        case BINARY:
          this.item = ByteConverter.float4(item, 0);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
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

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return Float.toString(resultSet.getFloat(position)).getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return convertToPG(resultSet.getFloat(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
