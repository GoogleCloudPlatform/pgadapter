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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translate wire protocol to array. Since arrays house any other specified types (including
 * potentially arrays), we use all parser types to parse each item within.
 */
public class ArrayParser extends Parser<List> {

  private static String STRING_TOGGLE = "\"";
  private static String ARRAY_DELIMITER = ",";
  private static String PG_ARRAY_OPEN = "{", PG_ARRAY_CLOSE = "}";
  private static String SPANNER_ARRAY_OPEN = "[", SPANNER_ARRAY_CLOSE = "]";

  int arrayType;
  boolean isStringEquivalent;

  public ArrayParser(ResultSet item, int position) throws SQLException {
    this.item = Arrays.asList((Object[]) item.getArray(position).getArray());
    this.arrayType = item.getArray(position).getBaseType();
    if (this.arrayType == Types.ARRAY) {
      throw new IllegalArgumentException(
          "Spanner does not support embedded Arrays."
              + " If you are seeing this, something went wrong!");
    }
    this.isStringEquivalent = stringEquivalence(this.arrayType);
  }

  @Override
  public List getItem() {
    return this.item;
  }

  /**
   * Whether a type is represented as string.
   *
   * @param arrayType The type to check.
   * @return True if the type uses strings, false otherwise.
   */
  private boolean stringEquivalence(int arrayType) {
    return arrayType == Types.BINARY
        || arrayType == Types.DATE
        || arrayType == Types.NVARCHAR
        || arrayType == Types.TIMESTAMP;
  }

  /**
   * Put quotes around the item if it is string equivalent, otherwise do not modify it.
   *
   * @param value The value to stringify.
   * @return Value with quotes around if string, otherwise just the item.
   */
  private String stringify(String value) {
    if (this.isStringEquivalent) {
      return STRING_TOGGLE + value + STRING_TOGGLE;
    }
    return value;
  }

  @Override
  protected String stringParse() {
    List<String> results = new LinkedList<>();
    for (Object currentItem : this.item) {
      results.add(stringify(Parser.create(currentItem, this.arrayType).stringParse()));
    }
    return results.stream()
        .collect(Collectors.joining(ARRAY_DELIMITER, PG_ARRAY_OPEN, PG_ARRAY_CLOSE));
  }

  @Override
  protected String spannerParse() {
    List<String> results = new LinkedList<>();
    for (Object currentItem : this.item) {
      results.add(stringify(Parser.create(currentItem, this.arrayType).spannerParse()));
    }
    return results.stream()
        .collect(Collectors.joining(ARRAY_DELIMITER, SPANNER_ARRAY_OPEN, SPANNER_ARRAY_CLOSE));
  }

  @Override
  protected byte[] binaryParse() {
    ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
    try {
      arrayStream.write(toBinary(1, Types.INTEGER)); // dimension
      arrayStream.write(toBinary(1, Types.INTEGER)); // Set null flag
      arrayStream.write(toBinary(this.arrayType, Types.INTEGER)); // Set type
      arrayStream.write(toBinary(this.item.size(), Types.INTEGER)); // Set array length
      arrayStream.write(toBinary(0, Types.INTEGER)); // Lower bound (?)
      for (Object currentItem : this.item) {
        if (currentItem == null) {
          arrayStream.write(toBinary(-1, Types.INTEGER));
        } else {
          byte[] data = Parser.create(currentItem, this.arrayType).binaryParse();
          arrayStream.write(toBinary(data.length, Types.INTEGER));
          arrayStream.write(data);
        }
      }
      return arrayStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
