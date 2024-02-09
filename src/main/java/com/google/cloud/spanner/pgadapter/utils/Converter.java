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

import static com.google.cloud.spanner.pgadapter.statements.CopyToStatement.COPY_BINARY_HEADER;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
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
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.CopyToStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Utility class for converting between generic PostgreSQL conversions. */
@InternalApi
public class Converter implements AutoCloseable {
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
  private final DataOutputStream outputStream = new DataOutputStream(buffer);
  private final IntermediateStatement statement;
  private final QueryMode mode;
  private final OptionsMetadata options;
  private final ResultSet resultSet;
  private final SessionState sessionState;
  private boolean includeBinaryCopyHeaderInFirstRow;
  private boolean firstRow = true;

  public Converter(
      IntermediateStatement statement,
      QueryMode mode,
      OptionsMetadata options,
      ResultSet resultSet,
      boolean includeBinaryCopyHeaderInFirstRow) {
    this.statement = statement;
    this.mode = mode;
    this.options = options;
    this.resultSet = resultSet;
    this.sessionState =
        statement
            .getConnectionHandler()
            .getExtendedQueryProtocolHandler()
            .getBackendConnection()
            .getSessionState();
    this.includeBinaryCopyHeaderInFirstRow = includeBinaryCopyHeaderInFirstRow;
  }

  public Converter includeBinaryCopyHeader() {
    this.includeBinaryCopyHeaderInFirstRow = true;
    return this;
  }

  public boolean isIncludeBinaryCopyHeaderInFirstRow() {
    return this.includeBinaryCopyHeaderInFirstRow;
  }

  @Override
  public void close() throws Exception {
    buffer.close();
    outputStream.close();
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public int convertResultSetRowToDataRowResponse() throws IOException {
    DataFormat fixedFormat = null;
    if (statement instanceof CopyToStatement) {
      fixedFormat =
          ((CopyToStatement) statement).isBinary()
              ? DataFormat.POSTGRESQL_BINARY
              : DataFormat.POSTGRESQL_TEXT;
    }
    buffer.reset();
    if (includeBinaryCopyHeaderInFirstRow && firstRow) {
      outputStream.write(COPY_BINARY_HEADER);
      outputStream.writeInt(0); // flags
      outputStream.writeInt(0); // header extension area length
    }
    firstRow = false;
    outputStream.writeShort(resultSet.getColumnCount());
    for (int column_index = 0; /* column indices start at 0 */
        column_index < resultSet.getColumnCount();
        column_index++) {
      if (resultSet.isNull(column_index)) {
        outputStream.writeInt(-1);
      } else {
        DataFormat format =
            fixedFormat == null
                ? DataFormat.getDataFormat(column_index, statement, mode, options)
                : fixedFormat;
        byte[] column =
            Converter.convertToPG(outputStream, this.resultSet, column_index, format, sessionState);
        if (column != null) {
          outputStream.writeInt(column.length);
          outputStream.write(column);
        }
      }
    }
    return buffer.size();
  }

  public void writeBuffer(DataOutputStream outputStream) throws IOException {
    buffer.writeTo(outputStream);
  }

  /**
   * Return the data of the specified column of the {@link ResultSet} as a byte array. The column
   * may not contain a null value.
   *
   * @param result The {@link ResultSet} to read the data from.
   * @param position The column index.
   * @param format The {@link DataFormat} format to use to encode the data.
   * @return a byte array containing the data in the specified format.
   */
  public static byte[] convertToPG(
      ResultSet result, int position, DataFormat format, SessionState sessionState) {
    return convertToPG(null, result, position, format, sessionState);
  }

  public static byte[] convertToPG(
      DataOutputStream outputStream,
      ResultSet result,
      int position,
      DataFormat format,
      SessionState sessionState) {
    Preconditions.checkArgument(!result.isNull(position), "Column may not contain a null value");
    Type type = result.getColumnType(position);
    switch (type.getCode()) {
      case BOOL:
        return BooleanParser.convertToPG(result, position, format);
      case BYTES:
        return BinaryParser.convertToPG(sessionState, outputStream, result, position, format);
      case DATE:
        return DateParser.convertToPG(result, position, format);
      case FLOAT64:
        return DoubleParser.convertToPG(result, position, format);
      case INT64:
        return LongParser.convertToPG(result, position, format);
      case PG_NUMERIC:
        return NumericParser.convertToPG(result, position, format);
      case STRING:
        return StringParser.convertToPG(sessionState, outputStream, result, position);
      case TIMESTAMP:
        return TimestampParser.convertToPG(result, position, format, sessionState.getTimezone());
      case PG_JSONB:
        return JsonbParser.convertToPG(sessionState, outputStream, result, position, format);
      case ARRAY:
        ArrayParser arrayParser = new ArrayParser(result, position, sessionState);
        return arrayParser.parse(format);
      case NUMERIC:
      case JSON:
      case STRUCT:
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + type);
    }
  }
}
