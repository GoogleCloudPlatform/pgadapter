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
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.text.StringEscapeUtils;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

/**
 * Translate wire protocol to array. Since arrays house any other specified types (including
 * potentially arrays), we use all parser types to parse each item within.
 */
public class ArrayParser extends Parser<List<?>> {
  private static final String STRING_TOGGLE = "\"";
  private static final String ARRAY_DELIMITER = ",";
  private static final String PG_ARRAY_OPEN = "{", PG_ARRAY_CLOSE = "}";
  private static final String SPANNER_ARRAY_OPEN = "[", SPANNER_ARRAY_CLOSE = "]";

  private final Type arrayElementType;
  private final int elementOid;
  private final boolean isStringEquivalent;
  private final SessionState sessionState;

  public ArrayParser(ResultSet item, int position, SessionState sessionState) {
    Preconditions.checkArgument(!item.isNull(position));
    this.sessionState = sessionState;
    this.arrayElementType = item.getColumnType(position).getArrayElementType();
    this.elementOid = Parser.toOid(item.getColumnType(position).getArrayElementType());
    if (this.arrayElementType.getCode() == Code.ARRAY) {
      throw new IllegalArgumentException(
          "Spanner does not support embedded Arrays."
              + " If you are seeing this, something went wrong!");
    }
    this.item = toList(item.getValue(position), this.arrayElementType.getCode());
    this.isStringEquivalent = stringEquivalence(this.arrayElementType.getCode());
  }

  public ArrayParser(
      byte[] item,
      FormatCode formatCode,
      SessionState sessionState,
      Type arrayElementType,
      int elementOid) {
    this.sessionState = sessionState;
    this.arrayElementType = arrayElementType;
    this.elementOid = elementOid;
    this.isStringEquivalent = stringEquivalence(elementOid);
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          this.item =
              stringArrayToList(
                  new String(item, StandardCharsets.UTF_8), elementOid, this.sessionState, false);
          break;
        case BINARY:
          this.item = binaryArrayToList(item, false);
          break;
        default:
      }
    }
  }

  private List<?> toList(Value value, Code arrayElementType) {
    switch (arrayElementType) {
      case BOOL:
        return value.getBoolArray();
      case DATE:
        return value.getDateArray();
      case PG_JSONB:
        return value.getPgJsonbArray();
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
      case FLOAT32:
        return value.getFloat32Array();
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

  /** Converts an array literal to the corresponding list of objects. */
  public static List<?> stringArrayToList(
      @Nullable String value,
      int elementOid,
      SessionState sessionState,
      boolean convertToValidSpannerElements) {
    if (value == null) {
      return null;
    }
    List<String> values = SimpleParser.readArrayLiteral(value, elementOid == Oid.BYTEA);
    ArrayList<Object> result = new ArrayList<>(values.size());
    for (String element : values) {
      if (element == null) {
        result.add(null);
      } else {
        Object parsedElement =
            Parser.create(
                    sessionState,
                    element.getBytes(StandardCharsets.UTF_8),
                    elementOid,
                    FormatCode.TEXT)
                .item;
        result.add(
            convertToValidSpannerElements
                ? toValidSpannerElement(parsedElement, elementOid)
                : parsedElement);
      }
    }
    return result;
  }

  /** Converts the given binary array value into a list of objects. */
  public static List<?> binaryArrayToList(
      @Nullable byte[] value, boolean convertToValidSpannerElements) {
    if (value == null) {
      return null;
    }
    byte[] buffer = new byte[20];
    try (DataInputStream dataStream = new DataInputStream(new ByteArrayInputStream(value))) {
      dataStream.readFully(buffer);
      int dimensions = ByteConverter.int4(buffer, 0);
      if (dimensions != 1) {
        throw PGExceptionFactory.newPGException(
            "Only single-dimension arrays are supported", SQLState.InvalidParameterValue);
      }
      // Null flag indicates whether there is at least one null element in the array. This is
      // ignored by PGAdapter, as we check for null elements in the conversion function below.
      int nullFlag = ByteConverter.int4(buffer, 4);
      int oid = ByteConverter.int4(buffer, 8);
      int size = ByteConverter.int4(buffer, 12);
      // Lower bound indicates whether the lower bound of the array is 1 or 0. This is irrelevant
      // for Cloud Spanner.
      int lowerBound = ByteConverter.int4(buffer, 16);
      ArrayList<Object> result = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        buffer = new byte[4];
        dataStream.readFully(buffer);
        int elementSize = ByteConverter.int4(buffer, 0);
        if (elementSize == -1) {
          result.add(null);
        } else {
          buffer = new byte[elementSize];
          dataStream.readFully(buffer);
          Object element = Parser.create(null, buffer, oid, FormatCode.BINARY).item;
          result.add(convertToValidSpannerElements ? toValidSpannerElement(element, oid) : element);
        }
      }
      return result;
    } catch (IOException exception) {
      throw PGException.newBuilder("Invalid array value")
          .setSQLState(SQLState.InvalidParameterValue)
          .setCause(exception)
          .build();
    }
  }

  private static Object toValidSpannerElement(@Nonnull Object value, int elementOid) {
    switch (elementOid) {
      case Oid.INT2:
        return ((Short) value).longValue();
      case Oid.INT4:
        return ((Integer) value).longValue();
    }
    return value;
  }

  @Override
  public List<?> getItem() {
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
        || arrayElementType == Code.TIMESTAMP
        || arrayElementType == Code.PG_JSONB;
  }

  /**
   * Whether a type is represented as string.
   *
   * @param arrayElementTypeOid The type to check.
   * @return True if the type uses strings, false otherwise.
   */
  private boolean stringEquivalence(int arrayElementTypeOid) {
    switch (arrayElementTypeOid) {
      case Oid.BYTEA:
      case Oid.DATE:
      case Oid.TIMESTAMPTZ:
      case Oid.TIMESTAMP:
      case Oid.VARCHAR:
      case Oid.UUID:
      case Oid.TEXT:
      case Oid.JSONB:
        return true;
      default:
        return false;
    }
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
      value = StringEscapeUtils.escapeJava(value);
      return STRING_TOGGLE + value + STRING_TOGGLE;
    }
    return value;
  }

  @Override
  public String stringParse() {
    if (this.item == null) {
      return null;
    }
    List<String> results = new LinkedList<>();
    for (Object currentItem : this.item) {
      results.add(
          stringify(
              Parser.create(currentItem, this.arrayElementType.getCode(), sessionState)
                  .stringParse()));
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
          stringify(
              Parser.create(currentItem, this.arrayElementType.getCode(), sessionState)
                  .spannerParse()));
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
      // Use lower bound 1 for all arrays. This is the only possible value in the text format, as
      // it does not have a field for the lower bound. It is also the default that is used by PG.
      arrayStream.write(IntegerParser.binaryParse(1)); // Lower bound.
      for (Object currentItem : this.item) {
        if (currentItem == null) {
          arrayStream.write(IntegerParser.binaryParse(-1));
        } else {
          byte[] data =
              Parser.create(currentItem, this.arrayElementType.getCode(), sessionState)
                  .binaryParse();
          arrayStream.write(IntegerParser.binaryParse(data.length));
          arrayStream.write(data);
        }
      }
      return arrayStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    switch (elementOid) {
      case Oid.BIT:
      case Oid.BOOL:
        statementBuilder.bind(name).toBoolArray((List<Boolean>) this.item);
        break;
      case Oid.INT2:
        if (this.item == null) {
          statementBuilder.bind(name).toInt64Array((long[]) null);
        } else {
          statementBuilder
              .bind(name)
              .toInt64Array(
                  ((List<Short>) this.item)
                      .stream()
                          .map(s -> s == null ? null : s.longValue())
                          .collect(Collectors.toList()));
        }
        break;
      case Oid.INT4:
        if (this.item == null) {
          statementBuilder.bind(name).toInt64Array((long[]) null);
        } else {
          statementBuilder
              .bind(name)
              .toInt64Array(
                  ((List<Integer>) this.item)
                      .stream()
                          .map(i -> i == null ? null : i.longValue())
                          .collect(Collectors.toList()));
        }
        break;
      case Oid.INT8:
        statementBuilder.bind(name).toInt64Array((List<Long>) this.item);
        break;
      case Oid.NUMERIC:
        statementBuilder.bind(name).toPgNumericArray((List<String>) this.item);
        break;
      case Oid.FLOAT4:
        statementBuilder.bind(name).toFloat32Array((List<Float>) this.item);
        break;
      case Oid.FLOAT8:
        statementBuilder.bind(name).toFloat64Array((List<Double>) this.item);
        break;
      case Oid.UUID:
      case Oid.VARCHAR:
      case Oid.TEXT:
        statementBuilder.bind(name).toStringArray((List<String>) this.item);
        break;
      case Oid.JSONB:
        statementBuilder.bind(name).toPgJsonbArray((List<String>) this.item);
        break;
      case Oid.BYTEA:
        statementBuilder.bind(name).toBytesArray((List<ByteArray>) this.item);
        break;
      case Oid.TIMESTAMPTZ:
      case Oid.TIMESTAMP:
        statementBuilder.bind(name).toTimestampArray((List<Timestamp>) this.item);
        break;
      case Oid.DATE:
        statementBuilder.bind(name).toDateArray((List<Date>) this.item);
        break;
      default:
        throw PGExceptionFactory.newPGException(
            "Unsupported array element type: " + arrayElementType, SQLState.InvalidParameterValue);
    }
  }
}
