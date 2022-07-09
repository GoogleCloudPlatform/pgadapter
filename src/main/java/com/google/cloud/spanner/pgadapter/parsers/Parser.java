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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.postgresql.core.Oid;

/**
 * Parser is the parsing superclass, used to take wire format data types and convert them
 * specifically to desired byte types. Each subclass is assigned with a specific type.
 */
@InternalApi
public abstract class Parser<T> {

  public enum FormatCode {
    TEXT,
    BINARY;

    public static FormatCode of(short code) {
      FormatCode[] values = FormatCode.values();
      if (code < 0 || code > values.length) {
        throw new IllegalArgumentException("Unknown format code: " + code);
      }
      return values[code];
    }
  }

  public static final long PG_EPOCH_SECONDS = 946684800L;
  public static final long PG_EPOCH_DAYS = PG_EPOCH_SECONDS / 86400L;
  protected static final Charset UTF8 = StandardCharsets.UTF_8;
  protected T item;

  /**
   * TODO: Remove when we have full support for DescribeStatement.
   *
   * <p>Guess the type of a parameter with unspecified type.
   *
   * @param item The value to guess the type for
   * @param formatCode The encoding that is used for the value
   * @return The {@link Oid} type code that is guessed for the value or {@link Oid#UNSPECIFIED} if
   *     no type could be guessed.
   */
  private static int guessType(byte[] item, FormatCode formatCode) {
    // TODO: Put 'guessType' behind a command line flag so it is only enabled when wanted.
    if (formatCode == FormatCode.TEXT && item != null) {
      String value = new String(item, StandardCharsets.UTF_8);
      if (TimestampParser.isTimestamp(value)) {
        return Oid.TIMESTAMPTZ;
      }
    }
    return Oid.UNSPECIFIED;
  }

  /**
   * Factory method to create a Parser subtype with a designated type from a byte array.
   *
   * @param item The data to be parsed
   * @param oidType The type of the designated data
   * @param formatCode The format of the data to be parsed
   * @return The parser object for the designated data type.
   */
  public static Parser<?> create(byte[] item, int oidType, FormatCode formatCode) {
    switch (oidType) {
      case Oid.BOOL:
      case Oid.BIT:
        return new BooleanParser(item, formatCode);
      case Oid.BYTEA:
      case Oid.BIT_ARRAY:
        return new BinaryParser(item, formatCode);
      case Oid.DATE:
        return new DateParser(item, formatCode);
      case Oid.FLOAT4:
        return new FloatParser(item, formatCode);
      case Oid.FLOAT8:
        return new DoubleParser(item, formatCode);
      case Oid.INT2:
        return new ShortParser(item, formatCode);
      case Oid.INT4:
        return new IntegerParser(item, formatCode);
      case Oid.INT8:
        return new LongParser(item, formatCode);
      case Oid.NUMERIC:
        return new NumericParser(item, formatCode);
      case Oid.TEXT:
      case Oid.VARCHAR:
        return new StringParser(item, formatCode);
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        return new TimestampParser(item, formatCode);
      case Oid.UNSPECIFIED:
        // Try to guess the type based on the value. Use an unspecified parser if no type could be
        // determined.
        int type = guessType(item, formatCode);
        if (type == Oid.UNSPECIFIED) {
          return new UnspecifiedParser(item, formatCode);
        }
        return create(item, type, formatCode);
      default:
        throw new IllegalArgumentException("Unsupported parameter type: " + oidType);
    }
  }

  /**
   * Factory method for parsing given a result set, for the specified column.
   *
   * @param result The result set object containing all data retrieved.
   * @param type The type of the data to be parsed.
   * @param columnarPosition Column from the result to be parsed.
   * @return The parser object for the designated data type.
   */
  public static Parser<?> create(ResultSet result, Type type, int columnarPosition) {
    switch (type.getCode()) {
      case BOOL:
        return new BooleanParser(result, columnarPosition);
      case BYTES:
        return new BinaryParser(result, columnarPosition);
      case DATE:
        return new DateParser(result, columnarPosition);
      case FLOAT64:
        return new DoubleParser(result, columnarPosition);
      case INT64:
        return new LongParser(result, columnarPosition);
      case PG_NUMERIC:
        return new NumericParser(result, columnarPosition);
      case STRING:
        return new StringParser(result, columnarPosition);
      case TIMESTAMP:
        return new TimestampParser(result, columnarPosition);
      case ARRAY:
        return new ArrayParser(result, columnarPosition);
      case NUMERIC:
      case STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + type);
    }
  }

  /**
   * Factory method for parsing generic data given a specified type.
   *
   * @param result The generic object to parse.
   * @param typeCode The type of the object to be parsed.
   * @return The parser object for the designated data type.
   */
  protected static Parser<?> create(Object result, Code typeCode) {
    switch (typeCode) {
      case BOOL:
        return new BooleanParser(result);
      case BYTES:
        return new BinaryParser(result);
      case DATE:
        return new DateParser(result);
      case FLOAT64:
        return new DoubleParser(result);
      case INT64:
        return new LongParser(result);
      case PG_NUMERIC:
        return new NumericParser(result);
      case STRING:
        return new StringParser(result);
      case TIMESTAMP:
        return new TimestampParser(result);
      case NUMERIC:
      case ARRAY:
      case STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + typeCode);
    }
  }

  /**
   * Translates the given Cloud Spanner {@link Type} to a PostgreSQL OID constant.
   *
   * @param type the type to translate
   * @return The OID constant value for the type
   */
  public static int toOid(Type type) {
    switch (type.getCode()) {
      case BOOL:
        return Oid.BOOL;
      case INT64:
        return Oid.INT8;
      case PG_NUMERIC:
        return Oid.NUMERIC;
      case FLOAT64:
        return Oid.FLOAT8;
      case STRING:
        return Oid.VARCHAR;
      case JSON:
        return Oid.JSONB;
      case BYTES:
        return Oid.BYTEA;
      case TIMESTAMP:
        return Oid.TIMESTAMPTZ;
      case DATE:
        return Oid.DATE;
      case ARRAY:
        switch (type.getArrayElementType().getCode()) {
          case BOOL:
            return Oid.BOOL_ARRAY;
          case INT64:
            return Oid.INT8_ARRAY;
          case PG_NUMERIC:
            return Oid.NUMERIC_ARRAY;
          case FLOAT64:
            return Oid.FLOAT8_ARRAY;
          case STRING:
            return Oid.VARCHAR_ARRAY;
          case JSON:
            return Oid.JSONB_ARRAY;
          case BYTES:
            return Oid.BYTEA_ARRAY;
          case TIMESTAMP:
            return Oid.TIMESTAMP_ARRAY;
          case DATE:
            return Oid.DATE_ARRAY;
          case NUMERIC:
          case ARRAY:
          case STRUCT:
          default:
            throw SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT, "Unsupported or unknown array type: " + type);
        }
      case NUMERIC:
      case STRUCT:
      default:
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Unsupported or unknown type: " + type);
    }
  }

  T getItem() {
    return this.item;
  }

  /**
   * Parses data based on specified data format (Spanner, text, or binary)
   *
   * @param format One of possible {@link DataFormat} types to parse data.
   * @return Byte format version of the input object.
   */
  public byte[] parse(DataFormat format) {
    switch (format) {
      case SPANNER:
        return this.spannerBinaryParse();
      case POSTGRESQL_TEXT:
        return this.stringBinaryParse();
      case POSTGRESQL_BINARY:
        return this.binaryParse();
      default:
        throw new IllegalArgumentException("Unknown format: " + format);
    }
  }

  /**
   * Used to parse data type into string. Override this to change the string representation.
   *
   * @return String representation of data.
   */
  public abstract String stringParse();

  /** @return Binary representation of string data. */
  protected byte[] stringBinaryParse() {
    return this.stringParse().getBytes(UTF8);
  }

  /**
   * Used to parse data type onto spanner format. Override this to change spanner representation.
   *
   * @return Spanner string representation of data.
   */
  protected String spannerParse() {
    return this.stringParse();
  }

  /** @return Binary representation of spanner string data. */
  protected byte[] spannerBinaryParse() {
    return this.spannerParse().getBytes(UTF8);
  }

  /** Used to parse data type onto binary. Override this to change binary representation. */
  protected abstract byte[] binaryParse();

  public abstract void bind(Statement.Builder statementBuilder, String name);
}
