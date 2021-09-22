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

import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.SQLMetadata;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** Utility class for converting between generic PostgreSQL conversions. */
public class Converter {

  /**
   * PostgreSQL parameters occur as $\\d+, whereas JDBC expects a question mark. For now, doing a
   * simple iteration to correct. Taking into account both quoted strings and escape sequences.
   *
   * @param sql The PostgreSQL String.
   * @return A {@link SQLMetadata} object containing both the corrected SQL String as well as the
   *     number of parameters iterated.
   */
  public static SQLMetadata toJDBCParams(String sql) {
    Preconditions.checkNotNull(sql);
    StringBuilder result = new StringBuilder();
    // Multimap from the 0-based parameter index to the associated set of JDBC parameter positions
    // (1-based).
    SetMultimap<Integer, Integer> parameterIndexToPositions = HashMultimap.create();
    final char SINGLE_QUOTE = '\'';
    final char DOUBLE_QUOTE = '"';
    final char DOLLAR = '$';
    final char BACK_SLASH = '\\';
    String currentTag = null;
    boolean isInQuoted = false;
    char startQuote = 0;
    boolean lastCharWasEscapeChar = false;
    int parameterOrder = 0;
    int totalParameterCount = 0;
    int index = 0;
    while (index < sql.length()) {
      char c = sql.charAt(index);
      if (isInQuoted) {
        if (c == startQuote) {
          if (c == DOLLAR) {
            // Check if this is the end of the current dollar quoted string.
            String tag = StatementParser.parseDollarQuotedString(sql, index + 1);
            if (tag != null && tag.equals(currentTag)) {
              index += tag.length() + 1;
              result.append(c);
              result.append(tag);
              isInQuoted = false;
              startQuote = 0;
            }
          } else if (lastCharWasEscapeChar) {
            lastCharWasEscapeChar = false;
          } else {
            isInQuoted = false;
            startQuote = 0;
          }
        } else if (c == BACK_SLASH) {
          lastCharWasEscapeChar = true;
        } else {
          lastCharWasEscapeChar = false;
        }
      } else {
        if (c == SINGLE_QUOTE || c == DOUBLE_QUOTE) {
          isInQuoted = true;
          startQuote = c;
        } else if (c == DOLLAR && sql.length() > index + 1) {
          if (Character.isDigit(sql.charAt(index + 1))) {
            // Consume the parameter index.
            int beginIndex = index + 1;
            result.append('?');
            while (++index < sql.length() && Character.isDigit(sql.charAt(index))) {}
            int parameterIndex = Integer.valueOf(sql.substring(beginIndex, index)) - 1;
            if (parameterIndex < 0) {
              throw new IllegalArgumentException("Parameter index should be >= 1");
            }
            parameterOrder++;
            parameterIndexToPositions.put(parameterIndex, parameterOrder);
            totalParameterCount = Integer.max(totalParameterCount, parameterIndex + 1);
            continue;
          }
          currentTag = StatementParser.parseDollarQuotedString(sql, index + 1);
          if (currentTag != null) {
            isInQuoted = true;
            startQuote = DOLLAR;
            index += currentTag.length() + 1;
            result.append(c);
            result.append(currentTag);
          }
        }
      }
      result.append(c);
      index++;
    }
    return new SQLMetadata(result.toString(), totalParameterCount, parameterIndexToPositions);
  }

  /**
   * Return the data of the specified column of the {@link ResultSet} as a byte array. The column
   * may not contain a null value.
   *
   * @param result The {@link ResultSet} to read the data from.
   * @param metadata The {@link ResultSetMetaData} object holding the metadata for the result.
   * @param columnarIndex The columnar index.
   * @param format The {@link DataFormat} format to use to encode the data.
   * @return a byte array containing the data in the specified format.
   */
  public byte[] parseData(
      ResultSet result, ResultSetMetaData metadata, int columnarIndex, DataFormat format)
      throws SQLException {
    Preconditions.checkArgument(
        result.getObject(columnarIndex) != null, "Column may not contain a null value");
    Parser parser = Parser.create(result, metadata.getColumnType(columnarIndex), columnarIndex);
    return parser.parse(format);
  }
}
