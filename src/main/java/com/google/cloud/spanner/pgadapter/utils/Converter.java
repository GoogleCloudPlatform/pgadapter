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
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.parsers.Parser;
import com.google.common.base.Preconditions;

/** Utility class for converting between generic PostgreSQL conversions. */
public class Converter {
  /**
   * Return the data of the specified column of the {@link ResultSet} as a byte array. The column
   * may not contain a null value.
   *
   * @param result The {@link ResultSet} to read the data from.
   * @param columnarIndex The columnar index.
   * @param format The {@link DataFormat} format to use to encode the data.
   * @return a byte array containing the data in the specified format.
   */
  public byte[] parseData(ResultSet result, int columnarIndex, DataFormat format) {
    Preconditions.checkArgument(
        !result.isNull(columnarIndex), "Column may not contain a null value");
    Parser<?> parser = Parser.create(result, result.getColumnType(columnarIndex), columnarIndex);
    return parser.parse(format);
  }
}
