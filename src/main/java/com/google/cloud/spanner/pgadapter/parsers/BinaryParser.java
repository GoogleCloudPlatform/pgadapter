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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import org.postgresql.util.PGbytea;

/**
 * Parse specified type to binary (generally this is the simplest parse class, as items are
 * generally represented in binary for wire format).
 */
public class BinaryParser extends Parser<ByteArray> {

  public BinaryParser(ResultSet item, int position) {
    this.item = item.getBytes(position);
  }

  public BinaryParser(Object item) {
    this.item = (ByteArray) item;
  }

  public BinaryParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          try {
            this.item = ByteArray.copyFrom(PGbytea.toBytes(item));
            break;
          } catch (SQLException e) {
            throw new IllegalArgumentException(
                "Invalid binary value: " + new String(item, StandardCharsets.UTF_8), e);
          }
        case BINARY:
          this.item = ByteArray.copyFrom(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.BINARY;
  }

  @Override
  protected String stringParse() {
    return this.item == null ? null : PGbytea.toPGString(this.item.toByteArray());
  }

  @Override
  protected byte[] spannerBinaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  @Override
  protected byte[] binaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item == null ? null : this.item);
  }
}
