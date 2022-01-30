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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Parse specified data to boolean. For most cases it is simply translating from chars 't'/'f' to
 * bit, or simply returning the bit representation.
 */
public class BooleanParser extends Parser<Boolean> {

  private static String TRUE_KEY = "t";
  private static String FALSE_KEY = "f";

  public BooleanParser(ResultSet item, int position) throws SQLException {
    this.item = item.getBoolean(position);
  }

  public BooleanParser(Object item) {
    this.item = (Boolean) item;
  }

  public BooleanParser(byte[] item, FormatCode formatCode) {
    switch (formatCode) {
      case TEXT:
        String stringValue = new String(item, UTF8);
        this.item = Boolean.valueOf(stringValue);
        break;
      case BINARY:
        if (!(item instanceof byte[])) {
          throw new IllegalArgumentException(
              "Unsupported input value for binary encoding: " + item.getClass());
        }
        this.item = ByteConverter.bool((byte[]) item, 0);
        break;
      default:
        throw new IllegalArgumentException("Unsupported format: " + formatCode);
    }
  }

  @Override
  public Boolean getItem() {
    return this.item;
  }

  @Override
  protected String stringParse() {
    return this.item ? TRUE_KEY : FALSE_KEY;
  }

  @Override
  protected String spannerParse() {
    return Boolean.toString(this.item);
  }
}
