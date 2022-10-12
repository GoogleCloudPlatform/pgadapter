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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.parsers.ArrayParser;
import com.google.cloud.spanner.pgadapter.parsers.BinaryParser;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.parsers.DateParser;
import com.google.cloud.spanner.pgadapter.parsers.DoubleParser;
import com.google.cloud.spanner.pgadapter.parsers.JsonbParser;
import com.google.cloud.spanner.pgadapter.parsers.LongParser;
import com.google.cloud.spanner.pgadapter.parsers.NumericParser;
import com.google.cloud.spanner.pgadapter.parsers.StringParser;
import com.google.cloud.spanner.pgadapter.parsers.TimestampParser;
import com.google.common.base.Preconditions;

/** Utility class for converting between generic PostgreSQL conversions. */
public class Converter {
  private Converter() {}
  /**
   * Return the data of the specified column of the {@link ResultSet} as a byte array. The column
   * may not contain a null value.
   *
   * @param result The {@link ResultSet} to read the data from.
   * @param position The column index.
   * @param format The {@link DataFormat} format to use to encode the data.
   * @return a byte array containing the data in the specified format.
   */
  public static byte[] convertToPG(ResultSet result, int position, DataFormat format) {
    Preconditions.checkArgument(!result.isNull(position), "Column may not contain a null value");
    Type type = result.getColumnType(position);
    switch (type.getCode()) {
      case BOOL:
        return BooleanParser.convertToPG(result, position, format);
      case BYTES:
        return BinaryParser.convertToPG(result, position, format);
      case DATE:
        return DateParser.convertToPG(result, position, format);
      case FLOAT64:
        return DoubleParser.convertToPG(result, position, format);
      case INT64:
        return LongParser.convertToPG(result, position, format);
      case PG_NUMERIC:
        return NumericParser.convertToPG(result, position, format);
      case STRING:
        return StringParser.convertToPG(result, position);
      case TIMESTAMP:
        return TimestampParser.convertToPG(result, position, format);
      case PG_JSONB:
        return JsonbParser.convertToPG(result, position, format);
      case ARRAY:
        ArrayParser arrayParser = new ArrayParser(result, position);
        return arrayParser.parse(format);
      case NUMERIC:
      case JSON:
      case STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + type);
    }
  }
}
