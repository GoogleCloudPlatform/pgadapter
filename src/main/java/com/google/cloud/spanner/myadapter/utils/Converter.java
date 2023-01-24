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

package com.google.cloud.spanner.myadapter.utils;

import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_BLOB;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_DATE;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_DATETIME;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_DECIMAL;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_DOUBLE;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_JSON;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_LONGLONG;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_TINYINT;
import static com.google.cloud.spanner.myadapter.utils.Converter.MySqlFieldTypes.MYSQL_TYPE_VAR_STRING;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.myadapter.parsers.Parser;
import com.google.cloud.spanner.myadapter.parsers.Parser.FormatCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Utility class for converting between generic MySQL conversions. */
public class Converter {

  public enum MySqlFieldTypes {
    MYSQL_TYPE_DECIMAL(0),
    MYSQL_TYPE_TINYINT(1),
    MYSQL_TYPE_SHORT(2),
    MYSQL_TYPE_LONG(3),
    MYSQL_TYPE_FLOAT(4),
    MYSQL_TYPE_DOUBLE(5),
    MYSQL_TYPE_NULL(6),
    MYSQL_TYPE_TIMESTAMP(7),
    MYSQL_TYPE_LONGLONG(8),
    MYSQL_TYPE_INT24(9),
    MYSQL_TYPE_DATE(10),
    MYSQL_TYPE_TIME(11),
    MYSQL_TYPE_DATETIME(12),
    MYSQL_TYPE_YEAR(13),
    MYSQL_TYPE_NEWDATE(14),
    MYSQL_TYPE_VARCHAR(15),
    MYSQL_TYPE_BIT(16),
    MYSQL_TYPE_TIMESTAMP2(17),
    MYSQL_TYPE_DATETIME2(18),
    MYSQL_TYPE_TIME2(19),
    MYSQL_TYPE_TYPED_ARRAY(20),
    MYSQL_TYPE_INVALID(243),
    MYSQL_TYPE_BOOL(244),
    MYSQL_TYPE_JSON(245),
    MYSQL_TYPE_NEWDECIMAL(246),
    MYSQL_TYPE_ENUM(247),
    MYSQL_TYPE_SET(248),
    MYSQL_TYPE_TINY_BLOB(249),
    MYSQL_TYPE_MEDIUM_BLOB(250),
    MYSQL_TYPE_LONG_BLOB(251),
    MYSQL_TYPE_BLOB(252),
    MYSQL_TYPE_VAR_STRING(253),
    MYSQL_TYPE_STRING(254),
    MYSQL_TYPE_GEOMETRY(255);

    int type;

    MySqlFieldTypes(int type) {
      this.type = type;
    }
  }

  public static byte[] convertResultSetRowToDataRowResponse(ResultSet resultSet)
      throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
    for (int i = 0; i < resultSet.getColumnCount(); ++i) {
      buffer.write(
          Parser.create(resultSet, resultSet.getColumnType(i), i).parse(FormatCode.LENGTH_ENCODED));
    }
    return buffer.toByteArray();
  }

  public static byte convertToMySqlCode(Type.Code spannerTypeCode) {
    switch (spannerTypeCode) {
      case BOOL:
        return (byte) MYSQL_TYPE_TINYINT.type;
      case INT64:
        return (byte) MYSQL_TYPE_LONGLONG.type;
      case STRING:
        return (byte) MYSQL_TYPE_VAR_STRING.type;
      case FLOAT64:
        return (byte) MYSQL_TYPE_DOUBLE.type;
      case BYTES:
        return (byte) MYSQL_TYPE_BLOB.type;
      case DATE:
        return (byte) MYSQL_TYPE_DATE.type;
      case TIMESTAMP:
        return (byte) MYSQL_TYPE_DATETIME.type;
      case NUMERIC:
        return (byte) MYSQL_TYPE_DECIMAL.type;
      case JSON:
        return (byte) MYSQL_TYPE_JSON.type;
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + spannerTypeCode);
    }
  }

  public static Type typeCodeToSpannerType(String spannerTypeCode) {
    return typeCodeToSpannerType(Enum.valueOf(Type.Code.class, spannerTypeCode));
  }

  public static Type typeCodeToSpannerType(Type.Code spannerTypeCode) {
    switch (spannerTypeCode) {
      case INT64:
        return Type.int64();
      case STRING:
        return Type.string();
      case FLOAT64:
        return Type.float64();
      case BYTES:
        return Type.bytes();
      case DATE:
        return Type.date();
      case TIMESTAMP:
        return Type.timestamp();
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + spannerTypeCode);
    }
  }
}
