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

package com.google.cloud.spanner.pgadapter.wireoutput;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.utils.Converter;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

/** Sends to the client specific row contents. */
@InternalApi
public class DataRowResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int COLUMN_NUMBER_LENGTH = 2;
  private static final int COLUMN_SIZE_LENGTH = 4;

  private final ResultSet resultSet;
  private final List<byte[]> columns;

  public DataRowResponse(
      DataOutputStream output,
      IntermediateStatement statement,
      ResultSet resultSet,
      OptionsMetadata options,
      QueryMode mode) {
    super(output, 0);
    this.resultSet = resultSet;
    this.columns = new ArrayList<>(this.resultSet.getColumnCount());
    this.length = HEADER_LENGTH + COLUMN_NUMBER_LENGTH;
    // TODO profile this with immense rows/column count to see if it's an aread of optimization.
    for (int column_index = 0; /* column indices start at 0 */
        column_index < this.resultSet.getColumnCount();
        column_index++) {
      length += COLUMN_SIZE_LENGTH;
      if (resultSet.isNull(column_index)) {
        columns.add(null);
      } else {
        DataFormat format = DataFormat.getDataFormat(column_index, statement, mode, options);
        byte[] column = Converter.parseData(this.resultSet, column_index, format);
        length += column.length;
        columns.add(column);
      }
    }
  }

  @Override
  protected void sendPayload() throws Exception {
    this.outputStream.writeShort(this.resultSet.getColumnCount());
    for (byte[] column_value : columns) {
      if (column_value == null) {
        this.outputStream.writeInt(-1);
      } else {
        this.outputStream.writeInt(column_value.length);
        this.outputStream.write(column_value);
      }
    }
  }

  @Override
  public byte getIdentifier() {
    return 'D';
  }

  @Override
  protected String getMessageName() {
    return "Data Row";
  }

  @Override
  protected String getPayloadString() {
    return "<REDACTED DUE TO LENGTH & PERFORMANCE CONSTRAINTS>";
  }
}
