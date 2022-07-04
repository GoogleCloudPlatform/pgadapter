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

public class CopyDataResponse extends WireOutput {

  private final byte[][] data;
  private final char delimiter;
  private final char rowTerminator;

  public CopyDataResponse(
      DataOutputStream output, byte[][] data, char delimiter, char rowTerminator) {
    super(output, calculateLength(data) + 4);
    this.data = data;
    this.delimiter = delimiter;
    this.rowTerminator = rowTerminator;
  }

  static int calculateLength(byte[][] data) {
    int length = 0;
    for (int i = 0; i < data.length; i++) {
      // +1 for the delimiter / row terminator
      length += data[i].length + 1;
    }
    return length;
  }

  @Override
  protected void sendPayload() throws Exception {
    for (int i = 0; i < data.length; i++) {
      this.outputStream.write(data[i]);
      if (i < (data.length - 1)) {
        this.outputStream.write(this.delimiter);
      }
    }
    this.outputStream.write(this.rowTerminator);
  }

  @Override
  public byte getIdentifier() {
    return 'd';
  }

  @Override
  protected String getMessageName() {
    return "Copy Data Response";
  }

  @Override
  protected String getPayloadString() {
    return "<REDACTED DUE TO LENGTH & PERFORMANCE CONSTRAINTS>";
  }
}
