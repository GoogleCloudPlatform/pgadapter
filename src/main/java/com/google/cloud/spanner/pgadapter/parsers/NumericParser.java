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
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to {@link Number}. */
public class NumericParser extends Parser<String> {
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
  public static String toNumericString(byte[] data) {
    Number number = ByteConverter.numeric(data, 0, data.length);
    return number == null ? null : number.toString();
  }

  @Override
  protected String stringParse() {
    return this.item;
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    if (this.item.equalsIgnoreCase("NaN")) {
      return "NaN".getBytes(StandardCharsets.UTF_8);
    }
    try {
      return ByteConverter.numeric(new BigDecimal(this.item));
    } catch (NumberFormatException exception) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, "Invalid numeric value: " + this.item);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(Value.pgNumeric(stringParse()));
  }
}
