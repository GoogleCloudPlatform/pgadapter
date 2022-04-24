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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to {@link Number}. */
// TODO: Change this to use String as type, and only convert to BigDecimal for binary transfer.
public class NumericParser extends Parser<Number> {
  public NumericParser(ResultSet item, int position) {
    // This should be either a BigDecimal value or a Double.NaN.
    Value value = item.getValue(position);
    if (Value.NAN.equalsIgnoreCase(value.getString())) {
      this.item = Double.NaN;
    } else {
      this.item = value.getNumeric();
    }
  }

  public NumericParser(Object item) {
    if (item instanceof Number) {
      this.item = (Number) item;
    } else if (item instanceof String) {
      String stringValue = (String) item;
      if (stringValue.equalsIgnoreCase("NaN")) {
        this.item = Double.NaN;
      } else {
        this.item = new BigDecimal(stringValue);
      }
    }
  }

  public NumericParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item);
          if (stringValue.equalsIgnoreCase("NaN")) {
            this.item = Double.NaN;
          } else {
            this.item = new BigDecimal(stringValue);
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
    if (this.item == null) {
      return null;
    }
    return Double.isNaN(this.item.doubleValue()) ? "NaN" : ((BigDecimal) this.item).toPlainString();
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    if (Double.isNaN(this.item.doubleValue())) {
      return "NaN".getBytes(StandardCharsets.UTF_8);
    }
    return ByteConverter.numeric((BigDecimal) this.item);
  }

  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(Value.pgNumeric(stringParse()));
  }
}
