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

/** Translate from wire protocol to long. */
public class LongParser extends Parser<Long> {

  public LongParser(ResultSet item, int position) {
    this.item = item.getLong(position);
  }

  public LongParser(Object item) {
    this.item = (Long) item;
  }

  public LongParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item = Long.valueOf(new String(item));
          break;
        case BINARY:
          this.item = ByteConverter.int8(item, 0);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.BIGINT;
  }

  @Override
  protected String stringParse() {
    return this.item == null ? null : Long.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    byte[] result = new byte[8];
    ByteConverter.int8(result, 0, this.item);
    return result;
  }

  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
