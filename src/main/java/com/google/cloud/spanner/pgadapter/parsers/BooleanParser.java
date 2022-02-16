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
import com.google.common.collect.ImmutableSet;
import java.sql.Types;
import java.util.Locale;
import java.util.Set;
import org.postgresql.util.ByteConverter;

/**
 * Parse specified data to boolean. For most cases it is simply translating from chars 't'/'f' to
 * bit, or simply returning the bit representation.
 */
public class BooleanParser extends Parser<Boolean> {
  private static final String TRUE_VALUE = "t";
  private static final String FALSE_VALUE = "f";
  // See https://www.postgresql.org/docs/current/datatype-boolean.html
  private static final Set<String> TRUE_VALUES =
      ImmutableSet.of("t", "tr", "tru", "true", "y", "ye", "yes", "on", "1");
  private static final Set<String> FALSE_VALUES =
      ImmutableSet.of("f", "fa", "fal", "fals", "false", "n", "no", "of", "off");

  public BooleanParser(ResultSet item, int position) {
    this.item = item.getBoolean(position);
  }

  public BooleanParser(Object item) {
    this.item = (Boolean) item;
  }

  public BooleanParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          String stringValue = new String(item, UTF8).toLowerCase(Locale.ENGLISH);
          if (TRUE_VALUES.contains(stringValue)) {
            this.item = true;
          } else if (FALSE_VALUES.contains(stringValue)) {
            this.item = false;
          } else {
            throw new IllegalArgumentException(stringValue + " is not a valid boolean value");
          }
          break;
        case BINARY:
          this.item = ByteConverter.bool(item, 0);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  @Override
  public int getSqlType() {
    return Types.BOOLEAN;
  }

  @Override
  protected String stringParse() {
    return this.item ? TRUE_VALUE : FALSE_VALUE;
  }

  @Override
  protected String spannerParse() {
    return Boolean.toString(this.item);
  }

  @Override
  protected byte[] binaryParse() {
    byte[] result = new byte[1];
    ByteConverter.bool(result, 0, this.item);
    return result;
  }

  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
