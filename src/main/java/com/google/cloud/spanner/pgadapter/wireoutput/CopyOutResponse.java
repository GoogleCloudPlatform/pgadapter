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

import static com.google.cloud.spanner.pgadapter.wireoutput.CopyInResponse.calculateLength;

import com.google.api.core.InternalApi;
import java.io.DataOutputStream;
import java.text.MessageFormat;
import java.util.Arrays;

@InternalApi
public class CopyOutResponse extends WireOutput {
  protected static final char IDENTIFIER = 'H';

  private final int numColumns;
  private final int formatCode;
  private final short[] columnFormat;

  public CopyOutResponse(DataOutputStream output, int numColumns, int formatCode) {
    super(output, calculateLength(numColumns));
    this.numColumns = numColumns;
    this.formatCode = formatCode;
    this.columnFormat = new short[numColumns];
    Arrays.fill(this.columnFormat, (short) formatCode);
  }

  @Override
  protected void sendPayload() throws Exception {
    this.outputStream.writeByte(this.formatCode);
    this.outputStream.writeShort(this.numColumns);
    for (int i = 0; i < columnFormat.length; i++) {
      this.outputStream.writeShort(columnFormat[i]);
    }
  }

  @Override
  public byte getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  protected String getMessageName() {
    return "Copy Out";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, Columns: {1}, Format: {2}")
        .format(new Object[] {this.length, this.numColumns, this.formatCode});
  }
}
