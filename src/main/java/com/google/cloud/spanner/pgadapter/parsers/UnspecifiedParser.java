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

import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Value;
import java.nio.charset.StandardCharsets;
import java.sql.Types;

/**
 * Parser for values with unspecified type. Any non-null values will be stored as a string, but the
 * SQL type will be reported as {@link Types#OTHER}.
 */
public class UnspecifiedParser extends Parser<Value> {

  public UnspecifiedParser(byte[] item, FormatCode formatCode) {
    this.item = item == null ? null : Value.string(new String(item, UTF8));
  }

  @Override
  public int getSqlType() {
    return Types.OTHER;
  }

  @Override
  protected String stringParse() {
    return this.item == null || this.item.isNull() ? null : this.item.getString();
  }

  @Override
  protected byte[] binaryParse() {
    return this.item == null || this.item.isNull()
        ? null
        : this.item.getString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void bind(Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
