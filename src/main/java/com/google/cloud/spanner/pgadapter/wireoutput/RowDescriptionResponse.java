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

import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.DataOutputStream;
import java.sql.ResultSetMetaData;
import java.text.MessageFormat;

/**
 * Sends back qualifier for a row.
 */
public class RowDescriptionResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_NUMBER_LENGTH = 2;
  private static final int TABLE_OID_LENGTH = 4;
  private static final int COLUMN_INDEX_LENGTH = 2;
  private static final int DATA_TYPE_OID_LENGTH = 4;
  private static final int DATA_TYPE_SIZE_LENGTH = 2;
  private static final int TYPE_MODIFIER_LENGTH = 4;
  private static final int FORMAT_CODE_LENGTH = 2;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte DEFAULT_FLAG = 0;

  private final IntermediateStatement statement;
  private final ResultSetMetaData metadata;
  private final QueryMode mode;
  private final OptionsMetadata options;
  private final int columnCount;

  public RowDescriptionResponse(DataOutputStream output,
      IntermediateStatement statement,
      ResultSetMetaData metadata,
      OptionsMetadata options,
      QueryMode mode) throws Exception {
    super(output, calculateLength(metadata));
    this.statement = statement;
    this.metadata = metadata;
    this.options = options;
    this.mode = mode;
    this.columnCount = metadata.getColumnCount();
  }

  private static int calculateLength(ResultSetMetaData metadata) throws Exception {
    int length = HEADER_LENGTH + FIELD_NUMBER_LENGTH;
    for (int column_index = 1;  /* columns start at 1 */
         column_index <= metadata.getColumnCount();
         column_index++) {
      length += metadata.getColumnName(column_index).length() + NULL_TERMINATOR_LENGTH
          + TABLE_OID_LENGTH + COLUMN_INDEX_LENGTH
          + DATA_TYPE_OID_LENGTH + DATA_TYPE_SIZE_LENGTH
          + TYPE_MODIFIER_LENGTH + FORMAT_CODE_LENGTH;
    }
    return length;
  }

  @Override
  protected void sendPayload() throws Exception {
      this.outputStream.writeShort(this.columnCount);
      DataFormat defaultFormat
          = DataFormat.getDataFormat(0, this.statement, this.mode, this.options);
      for (int column_index = 1; /* columns start at 1 */
          column_index <= this.columnCount;
          column_index++) {
        this.outputStream.write(this.metadata.getColumnName(column_index).getBytes(UTF8));
        // If it can be identified as a column of a table, the object ID of the table.
        this.outputStream.writeByte(DEFAULT_FLAG);
        // TODO: pass through Postgres types
        // If it can be identified as a column of a table, the attribute number of the column
        this.outputStream.writeInt(DEFAULT_FLAG);
        // The object ID of the field's data type.
        this.outputStream.writeShort(DEFAULT_FLAG);
        this.outputStream.writeInt(this.metadata.getColumnType(column_index));
        this.outputStream.writeShort(this.metadata.getColumnType(column_index));
        // The type modifier. The meaning of the modifier is type-specific.
        this.outputStream.writeInt(DEFAULT_FLAG);
        short format = this.statement == null ? defaultFormat.getCode()
            : this.statement.getResultFormatCode(column_index) == 0 ? defaultFormat.getCode() :
                this.statement.getResultFormatCode(column_index);
        this.outputStream.writeShort(format);
      }
      this.outputStream.flush();
  }


  @Override
  public byte getIdentifier() {
    return 'T';
  }

  @Override
  protected String getMessageName() {
    return "Row Description";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, "
            + "Columns Returned: {1}")
        .format(new Object[]{
            this.length,
            this.columnCount,
        });
  }
}
