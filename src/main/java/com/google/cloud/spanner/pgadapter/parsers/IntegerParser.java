// Copyright 2021 Google LLC
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

/** Translate from wire protocol to int. */
public class IntegerParser extends Parser<Integer> {

  public IntegerParser(ResultSet item, int position) {
    this.item = Math.toIntExact(item.getLong(position));
  }

  public IntegerParser(Object item) {
    this.item = (Integer) item;
  }

  public IntegerParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = Integer.valueOf(new String(item));
          break;
        case BINARY:
          this.item = ByteConverter.int4(item, 0);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.INTEGER;
  }

  @Override
  protected String stringParse() {
    return Integer.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    return binaryParse(this.item);
  }

  public static byte[] binaryParse(int value) {
    byte[] result = new byte[4];
    ByteConverter.int4(result, 0, value);
    return result;
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item == null ? null : this.item.longValue());
  }
}
