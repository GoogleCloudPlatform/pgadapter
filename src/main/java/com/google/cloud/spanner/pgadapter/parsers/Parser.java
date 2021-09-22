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

import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

/**
 * Parser is the parsing superclass, used to take wire format data types and convert them
 * specifically to desired byte types. Each subclass is assigned with a specific type.
 */
public abstract class Parser<T> {

  public static final long PG_EPOCH_SECONDS = 946684800L;
  public static final long PG_EPOCH_DAYS = PG_EPOCH_SECONDS / 86400L;
  protected static final Charset UTF8 = StandardCharsets.UTF_8;
  protected T item;

  /**
   * Factory method to create a Parser subtype with a designated type from a byte array.
   *
   * @param item The data to be parsed.
   * @param oidType The type of the designated data.
   * @return The parser object for the designated data type.
   */
  public static Parser create(byte[] item, int oidType) {
    switch (oidType) {
      case Oid.BOOL:
        return new BooleanParser(item);
      case Oid.BIT_ARRAY:
        return new BinaryParser(item);
      case Oid.DATE:
        return new DateParser(item);
      case Oid.FLOAT8:
        return new DoubleParser(item);
      case Oid.INT8:
        return new LongParser(item);
      case Oid.INT4:
        return new IntegerParser(item);
      case Oid.NUMERIC:
        return new StringParser(item);
      case Oid.TEXT:
      case Oid.UNSPECIFIED:
      case Oid.VARCHAR:
        return new StringParser(item);
      case Oid.TIMESTAMP:
        return new TimestampParser(item);
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + oidType);
    }
  }

  /**
   * Factory method for parsing given a result set, for the specified column.
   *
   * @param result The result set object containing all data retrieved.
   * @param oidType The type of the data to be parsed.
   * @param columnarPosition Column from the result to be parsed.
   * @return The parser object for the designated data type.
   * @throws SQLException if the result data type does not correspond with the desired type.
   */
  public static Parser create(ResultSet result, int oidType, int columnarPosition)
      throws SQLException {
    switch (oidType) {
      case Types.BOOLEAN:
        return new BooleanParser(result, columnarPosition);
      case Types.BINARY:
        return new BinaryParser(result, columnarPosition);
      case Types.DATE:
        return new DateParser(result, columnarPosition);
      case Types.DOUBLE:
        return new DoubleParser(result, columnarPosition);
      case Types.BIGINT:
        return new LongParser(result, columnarPosition);
      case Types.INTEGER:
        return new IntegerParser(result, columnarPosition);
      case Types.NUMERIC:
        return new StringParser(result, columnarPosition);
      case Types.NVARCHAR:
        return new StringParser(result, columnarPosition);
      case Types.TIMESTAMP:
        return new TimestampParser(result, columnarPosition);
      case Types.ARRAY:
        return new ArrayParser(result, columnarPosition);
      case Types.STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element oidType: " + oidType);
    }
  }

  /**
   * Factory method for parsing generic data given a specified type.
   *
   * @param result The generic object to parse.
   * @param oidType The type of the object to be parsed.
   * @return Theparser object for the designated data type.
   */
  protected static Parser create(Object result, int oidType) {
    switch (oidType) {
      case Types.BOOLEAN:
        return new BooleanParser(result);
      case Types.BINARY:
        return new BinaryParser(result);
      case Types.DATE:
        return new DateParser(result);
      case Types.DOUBLE:
        return new DoubleParser(result);
      case Types.BIGINT:
        return new LongParser(result);
      case Types.INTEGER:
        return new IntegerParser(result);
      case Types.NUMERIC:
        return new StringParser(result);
      case Types.NVARCHAR:
        return new StringParser(result);
      case Types.TIMESTAMP:
        return new TimestampParser(result);
      case Types.ARRAY:
      case Types.STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + oidType);
    }
  }

  /**
   * Convert a specified candidate type from specified type to binary.
   *
   * @param candidate Specified object to convert.
   * @param type Type of specified object.
   * @return Object in binary representation.
   */
  public static byte[] toBinary(Object candidate, int type) {
    byte[] result;
    switch (type) {
      case Types.BOOLEAN:
        result = new byte[1];
        ByteConverter.bool(result, 0, (Boolean) candidate);
        return result;
      case Types.INTEGER:
        result = new byte[4];
        ByteConverter.int4(result, 0, (Integer) candidate);
        return result;
      case Types.DOUBLE:
        result = new byte[8];
        ByteConverter.float8(result, 0, (Double) candidate);
        return result;
      case Types.BIGINT:
        result = new byte[8];
        ByteConverter.int8(result, 0, (Long) candidate);
        return result;
      default:
        throw new IllegalArgumentException("Type " + type + " is not valid!");
    }
  }

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
  protected abstract String stringParse();

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
  protected byte[] binaryParse() {
    return this.stringParse().getBytes(UTF8);
  }
}
