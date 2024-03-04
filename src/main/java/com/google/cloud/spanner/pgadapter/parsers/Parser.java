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
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.SessionState;
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
   * Factory method to create a Parser subtype with a designated type from a byte array.
   *
   * @param item The data to be parsed
   * @param oidType The type of the designated data
   * @param formatCode The format of the data to be parsed
   * @param sessionState The session state to use when parsing and converting
   * @return The parser object for the designated data type.
   */
  public static Parser<?> create(
      SessionState sessionState, byte[] item, int oidType, FormatCode formatCode) {
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
      case Oid.UUID:
        return new UuidParser(item, formatCode);
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        return new TimestampParser(item, formatCode, sessionState);
      case Oid.JSONB:
        return new JsonbParser(item, formatCode);

      case Oid.BOOL_ARRAY:
      case Oid.BYTEA_ARRAY:
      case Oid.DATE_ARRAY:
      case Oid.FLOAT4_ARRAY:
      case Oid.FLOAT8_ARRAY:
      case Oid.INT2_ARRAY:
      case Oid.INT4_ARRAY:
      case Oid.INT8_ARRAY:
      case Oid.NUMERIC_ARRAY:
      case Oid.TEXT_ARRAY:
      case Oid.VARCHAR_ARRAY:
      case Oid.UUID_ARRAY:
      case Oid.TIMESTAMP_ARRAY:
      case Oid.TIMESTAMPTZ_ARRAY:
      case Oid.JSONB_ARRAY:
        int elementOid = getArrayElementOid(oidType);
        return new ArrayParser(item, formatCode, sessionState, toType(elementOid), elementOid);

      case Oid.UNSPECIFIED:
      default:
        // Use the UnspecifiedParser for unknown types. This will encode the parameter value as a
        // string and send it to Spanner without any type information. This will ensure that clients
        // that for example send char instead of varchar as the type code for a parameter would
        // still work.
        return new UnspecifiedParser(item, formatCode);
    }
  }

  static int getArrayElementOid(int arrayOid) {
    switch (arrayOid) {
      case Oid.BOOL_ARRAY:
        return Oid.BOOL;
      case Oid.BYTEA_ARRAY:
        return Oid.BYTEA;
      case Oid.DATE_ARRAY:
        return Oid.DATE;
      case Oid.FLOAT4_ARRAY:
        return Oid.FLOAT4;
      case Oid.FLOAT8_ARRAY:
        return Oid.FLOAT8;
      case Oid.INT2_ARRAY:
        return Oid.INT2;
      case Oid.INT4_ARRAY:
        return Oid.INT4;
      case Oid.INT8_ARRAY:
        return Oid.INT8;
      case Oid.NUMERIC_ARRAY:
        return Oid.NUMERIC;
      case Oid.TEXT_ARRAY:
        return Oid.TEXT;
      case Oid.VARCHAR_ARRAY:
        return Oid.VARCHAR;
      case Oid.UUID_ARRAY:
        return Oid.UUID;
      case Oid.TIMESTAMP_ARRAY:
        return Oid.TIMESTAMP;
      case Oid.TIMESTAMPTZ_ARRAY:
        return Oid.TIMESTAMPTZ;
      case Oid.JSONB_ARRAY:
        return Oid.JSONB;
      default:
        return Oid.UNSPECIFIED;
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
  public static Parser<?> create(
      ResultSet result, Type type, int columnarPosition, SessionState sessionState) {
    switch (type.getCode()) {
      case BOOL:
        return new BooleanParser(result, columnarPosition);
      case BYTES:
        return new BinaryParser(result, columnarPosition);
      case DATE:
        return new DateParser(result, columnarPosition);
      case FLOAT32:
        return new FloatParser(result, columnarPosition);
      case FLOAT64:
        return new DoubleParser(result, columnarPosition);
      case INT64:
        return new LongParser(result, columnarPosition);
      case PG_NUMERIC:
        return new NumericParser(result, columnarPosition);
      case STRING:
        return new StringParser(result, columnarPosition);
      case TIMESTAMP:
        return new TimestampParser(result, columnarPosition, sessionState);
      case PG_JSONB:
        return new JsonbParser(result, columnarPosition);
      case ARRAY:
        return new ArrayParser(result, columnarPosition, sessionState);
      case NUMERIC:
      case JSON:
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
  protected static Parser<?> create(Object result, Code typeCode, SessionState sessionState) {
    switch (typeCode) {
      case BOOL:
        return new BooleanParser(result);
      case BYTES:
        return new BinaryParser(result);
      case DATE:
        return new DateParser(result);
      case FLOAT32:
        return new FloatParser(result);
      case FLOAT64:
        return new DoubleParser(result);
      case INT64:
        return new LongParser(result);
      case PG_NUMERIC:
        return new NumericParser(result);
      case STRING:
        return new StringParser(result);
      case TIMESTAMP:
        return new TimestampParser(result, sessionState);
      case PG_JSONB:
        return new JsonbParser(result);
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
      case FLOAT32:
        return Oid.FLOAT4;
      case FLOAT64:
        return Oid.FLOAT8;
      case STRING:
        return Oid.VARCHAR;
      case PG_JSONB:
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
          case FLOAT32:
            return Oid.FLOAT4_ARRAY;
          case FLOAT64:
            return Oid.FLOAT8_ARRAY;
          case STRING:
            return Oid.VARCHAR_ARRAY;
          case PG_JSONB:
            return Oid.JSONB_ARRAY;
          case BYTES:
            return Oid.BYTEA_ARRAY;
          case TIMESTAMP:
            return Oid.TIMESTAMPTZ_ARRAY;
          case DATE:
            return Oid.DATE_ARRAY;
          case NUMERIC:
          case JSON:
          case ARRAY:
          case STRUCT:
          default:
            throw PGExceptionFactory.newPGException(
                "Unsupported or unknown array type: " + type, SQLState.InternalError);
        }
      case NUMERIC:
      case JSON:
      case STRUCT:
      default:
        throw PGExceptionFactory.newPGException(
            "Unsupported or unknown type: " + type, SQLState.InternalError);
    }
  }

  public static Type toType(int oid) {
    switch (oid) {
      case Oid.BOOL:
      case Oid.BIT:
        return Type.bool();
      case Oid.BYTEA:
      case Oid.BIT_ARRAY:
        return Type.bytes();
      case Oid.DATE:
        return Type.date();
      case Oid.FLOAT4:
        return Type.float32();
      case Oid.FLOAT8:
        return Type.float64();
      case Oid.INT2:
      case Oid.INT4:
      case Oid.INT8:
        return Type.int64();
      case Oid.NUMERIC:
        return Type.pgNumeric();
      case Oid.TEXT:
      case Oid.VARCHAR:
      case Oid.UUID:
        return Type.string();
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        return Type.timestamp();
      case Oid.JSONB:
        return Type.pgJsonb();

      case Oid.BOOL_ARRAY:
      case Oid.BYTEA_ARRAY:
      case Oid.DATE_ARRAY:
      case Oid.FLOAT4_ARRAY:
      case Oid.FLOAT8_ARRAY:
      case Oid.INT2_ARRAY:
      case Oid.INT4_ARRAY:
      case Oid.INT8_ARRAY:
      case Oid.NUMERIC_ARRAY:
      case Oid.TEXT_ARRAY:
      case Oid.VARCHAR_ARRAY:
      case Oid.UUID_ARRAY:
      case Oid.TIMESTAMP_ARRAY:
      case Oid.TIMESTAMPTZ_ARRAY:
      case Oid.JSONB_ARRAY:
        return Type.array(toType(getArrayElementOid(oid)));

      case Oid.UNSPECIFIED:
      default:
        throw PGExceptionFactory.newPGException(
            "Unsupported or unknown OID: " + oid, SQLState.InvalidParameterValue);
    }
  }

  /**
   * Translates the given Cloud Spanner {@link Type} to a PostgreSQL OID constant.
   *
   * @param type the type to translate
   * @return The OID constant value for the type
   */
  public static int toOid(com.google.spanner.v1.Type type) {
    switch (type.getCode()) {
      case BOOL:
        return Oid.BOOL;
      case INT64:
        return Oid.INT8;
      case NUMERIC:
        return Oid.NUMERIC;
      case FLOAT32:
        return Oid.FLOAT4;
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
          case NUMERIC:
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
            return Oid.TIMESTAMPTZ_ARRAY;
          case DATE:
            return Oid.DATE_ARRAY;
          case ARRAY:
          case STRUCT:
          default:
            throw PGExceptionFactory.newPGException(
                "Unsupported or unknown array type: " + type, SQLState.InternalError);
        }
      case STRUCT:
      default:
        throw PGExceptionFactory.newPGException(
            "Unsupported or unknown type: " + type, SQLState.InternalError);
    }
  }

  /** Returns the item helder by this parser. */
  public T getItem() {
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
