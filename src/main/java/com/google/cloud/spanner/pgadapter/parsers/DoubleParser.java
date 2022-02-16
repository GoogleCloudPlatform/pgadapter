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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import java.sql.Types;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to double. */
public class DoubleParser extends Parser<Double> {

  public DoubleParser(ResultSet item, int position) {
    this.item = item.getDouble(position);
  }

  public DoubleParser(Object item) {
    this.item = (Double) item;
  }

  public DoubleParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = Double.valueOf(new String(item));
          break;
        case BINARY:
          this.item = ByteConverter.float8(item, 0);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.DOUBLE;
  }

  @Override
  protected String stringParse() {
    return this.item == null ? null : Double.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    byte[] result = new byte[8];
    ByteConverter.float8(result, 0, this.item);
    return result;
  }

  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
