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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translate wire protocol to array. Since arrays house any other specified types (including
 * potentially arrays), we use all parser types to parse each item within.
 */
class ArrayParser extends Parser<List<?>> {
  private static final String STRING_TOGGLE = "\"";
  private static final String ARRAY_DELIMITER = ",";
  private static final String PG_ARRAY_OPEN = "{", PG_ARRAY_CLOSE = "}";
  private static final String SPANNER_ARRAY_OPEN = "[", SPANNER_ARRAY_CLOSE = "]";

  private final Type arrayElementType;
  private final boolean isStringEquivalent;

  ArrayParser(ResultSet item, int position) {
    if (item != null) {
      this.arrayElementType = item.getColumnType(position).getArrayElementType();
      if (this.arrayElementType.getCode() == Code.ARRAY) {
        throw new IllegalArgumentException(
            "Spanner does not support embedded Arrays."
                + " If you are seeing this, something went wrong!");
      }
      this.item = toList(item.getValue(position), this.arrayElementType.getCode());
      this.isStringEquivalent = stringEquivalence(this.arrayElementType.getCode());
    } else {
      arrayElementType = null;
      isStringEquivalent = false;
    }
  }

  private List<?> toList(Value value, Code arrayElementType) {
    switch (arrayElementType) {
      case BOOL:
        return value.getBoolArray();
      case DATE:
        return value.getDateArray();
      case JSON:
        return value.getJsonArray();
      case BYTES:
        return value.getBytesArray();
      case INT64:
        return value.getInt64Array();
      case STRING:
      case PG_NUMERIC:
        // Get numeric arrays as a string array instead of as an array of BigDecimal, as numeric
        // arrays could contain 'NaN' values, which are not supported by BigDecimal.
        return value.getStringArray();
      case TIMESTAMP:
        return value.getTimestampArray();
      case FLOAT64:
        return value.getFloat64Array();
      case NUMERIC: // Only PG_NUMERIC is supported
      case ARRAY:
      case STRUCT:
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("Array of %s is not supported", arrayElementType));
    }
  }

  @Override
  List<?> getItem() {
    return this.item;
  }

  /**
   * Whether a type is represented as string.
   *
   * @param arrayElementType The type to check.
   * @return True if the type uses strings, false otherwise.
   */
  private boolean stringEquivalence(Code arrayElementType) {
    return arrayElementType == Code.BYTES
        || arrayElementType == Code.DATE
        || arrayElementType == Code.STRING
        || arrayElementType == Code.TIMESTAMP;
  }

  /**
   * Put quotes around the item if it is string equivalent, otherwise do not modify it.
   *
   * @param value The value to stringify.
   * @return Value with quotes around if string, otherwise just the item.
   */
  private String stringify(String value) {
    if (value == null) {
      // This must be upper-case.
      return "NULL";
    }
    if (this.isStringEquivalent) {
      if (value.indexOf('\\') > -1) {
        value = value.replace("\\", "\\\\");
      }
      return STRING_TOGGLE + value + STRING_TOGGLE;
    }
    return value;
  }

  @Override
  protected String stringParse() {
    if (this.item == null) {
      return null;
    }
    List<String> results = new LinkedList<>();
    for (Object currentItem : this.item) {
      results.add(
          stringify(Parser.create(currentItem, this.arrayElementType.getCode()).stringParse()));
    }
    return results.stream()
        .collect(Collectors.joining(ARRAY_DELIMITER, PG_ARRAY_OPEN, PG_ARRAY_CLOSE));
  }

  @Override
  protected String spannerParse() {
    if (this.item == null) {
      return null;
    }
    List<String> results = new LinkedList<>();
    for (Object currentItem : this.item) {
      results.add(
          stringify(Parser.create(currentItem, this.arrayElementType.getCode()).spannerParse()));
    }
    return results.stream()
        .collect(Collectors.joining(ARRAY_DELIMITER, SPANNER_ARRAY_OPEN, SPANNER_ARRAY_CLOSE));
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
    try {
      arrayStream.write(IntegerParser.binaryParse(1)); // dimension
      arrayStream.write(IntegerParser.binaryParse(1)); // Set null flag
      arrayStream.write(IntegerParser.binaryParse(Parser.toOid(this.arrayElementType))); // Set type
      arrayStream.write(IntegerParser.binaryParse(this.item.size())); // Set array length
      arrayStream.write(IntegerParser.binaryParse(0)); // Lower bound (?)
      for (Object currentItem : this.item) {
        if (currentItem == null) {
          arrayStream.write(IntegerParser.binaryParse(-1));
        } else {
          byte[] data = Parser.create(currentItem, this.arrayElementType.getCode()).binaryParse();
          arrayStream.write(IntegerParser.binaryParse(data.length));
          arrayStream.write(data);
        }
      }
      return arrayStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    throw SpannerExceptionFactory.newSpannerException(
        ErrorCode.UNIMPLEMENTED, "Array parameters not yet supported");
  }
}
