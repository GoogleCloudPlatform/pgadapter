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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to {@link Number}. */
public class NumericParser extends Parser<Number> {
  public NumericParser(ResultSet item, int position) throws SQLException {
    // This should be either a BigDecimal value or a Double.NaN.
    this.item = (Number) item.getObject(position);
  }

  public NumericParser(Object item) {
    this.item = (Number) item;
  }

  public NumericParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item);
          if (stringValue.equalsIgnoreCase("NaN")) {
            this.item = Double.NaN;
          } else {
            this.item = new BigDecimal(new String(item));
          }
          break;
        case BINARY:
          this.item = ByteConverter.numeric(item, 0, item.length);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.NUMERIC;
  }

  @Override
  protected String stringParse() {
    return Double.isNaN(this.item.doubleValue()) ? "NaN" : ((BigDecimal) this.item).toPlainString();
  }

  @Override
  protected byte[] binaryParse() {
    if (Double.isNaN(this.item.doubleValue())) {
      return "NaN".getBytes(StandardCharsets.UTF_8);
    }
    return ByteConverter.numeric((BigDecimal) this.item);
  }
}
