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

package com.google.cloud.spanner.myadapter.wireoutput;

import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import java.io.IOException;

public class EofResponse extends WireOutput {
  public EofResponse(int currentSequenceNumber, ConnectionMetadata connectionMetadata)
      throws IOException {
    super(currentSequenceNumber, connectionMetadata);

    byte[] eofIdentifier = new byte[] {(byte) 0xfe};
    writePayload(eofIdentifier);

    byte[] affectedRows = new byte[] {(byte) 0x00};
    writePayload(affectedRows);

    byte[] lastInsertId = new byte[] {(byte) 0x00};
    writePayload(lastInsertId);

    byte[] serverStatus = {(byte) 2, (byte) 0};
    writePayload(serverStatus);

    byte[] warnings = {(byte) 0, (byte) 0};
    writePayload(warnings);
  }

  @Override
  protected String getMessageName() {
    return "OkResponse";
  }

  @Override
  protected String getPayloadString() {
    return "";
  }
}
