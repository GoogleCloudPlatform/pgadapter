// Copyright 2022 Google LLC
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;

public class CopyInResponse extends WireOutput {

  /* Message format specification from https://www.postgresql.org/docs/13/protocol-message-formats.html */
  private static final int HEADER_LENGTH =
      4; // Int32 length of message contents in bytes, including self.
  private static final int FORMAT_FLAG_LENGTH =
      1; // Int8 overall COPY format (0 for text, 1 for binary)
  private static final int COLUMN_NUM_LENGTH =
      2; // Int16 number of columns in the data to be copied
  private static final int COLUMN_FORMAT_FLAG_LENGTH =
      2; // Int16 size for each format code in column format array

  protected static final char IDENTIFIER = 'G';

  private final int numColumns;
  private final int formatCode;
  private final byte[] columnFormat;

  public CopyInResponse(DataOutputStream output, int numColumns, int formatCode) {
    super(output, calculateLength(numColumns));
    this.numColumns = numColumns;
    this.formatCode = formatCode;
    columnFormat = new byte[COLUMN_NUM_LENGTH * this.numColumns];
    Arrays.fill(columnFormat, (byte) formatCode);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(this.formatCode);
    this.outputStream.writeShort(this.numColumns);
    this.outputStream.write(this.columnFormat);
    this.outputStream.flush();
  }

  @Override
  public byte getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  protected String getMessageName() {
    return "Copy In";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Columns Requested: {1}")
        .format(
            new Object[] {
              this.length, this.numColumns,
            });
  }

  private static int calculateLength(int columnCount) {
    return HEADER_LENGTH
        + FORMAT_FLAG_LENGTH
        + COLUMN_NUM_LENGTH
        + (COLUMN_FORMAT_FLAG_LENGTH * columnCount);
  }
}
