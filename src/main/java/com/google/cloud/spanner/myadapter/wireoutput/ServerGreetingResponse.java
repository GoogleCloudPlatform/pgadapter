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
import com.google.cloud.spanner.myadapter.parsers.IntegerParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ServerGreetingResponse extends WireOutput {

  public ServerGreetingResponse(int currentSequenceNumber, ConnectionMetadata connectionMetadata,
      String serverVersion)
      throws IOException {
    super(currentSequenceNumber, connectionMetadata);

    byte[] protocol = new byte[]{(byte) 10};
    writePayload(protocol);
    serverVersion = serverVersion + "\0";
    writePayload(serverVersion.getBytes(StandardCharsets.UTF_8));
    int threadId = 0;
    writePayload(IntegerParser.binaryParse(threadId));
    byte[] salt = new byte[8];
    writePayload(salt); // salt
    byte[] filler = new byte[]{(byte) 0x00}; // ???
    writePayload(filler);
    byte[] serverCapabilities = {(byte) 255, (byte) 255};
    writePayload(serverCapabilities);
    byte[] charSet = new byte[]{(byte) 255};
    writePayload(charSet);
    byte[] serverStatus = {(byte) 2, (byte) 0};
    writePayload(serverStatus);
    byte[] eServerCapabilities = {(byte) 255, (byte) 223};
    writePayload(eServerCapabilities);
    int authPluginDataLength = 21;
    writePayload(new byte[]{(byte) authPluginDataLength});
    byte[] reserved = new byte[10];
    writePayload(reserved);
    byte[] extendedSalt = new byte[13];
    writePayload(extendedSalt);
    byte[] authPluginName = "caching_sha2_password".getBytes(StandardCharsets.UTF_8);
    writePayload(authPluginName);
    byte[] nullTerminator = new byte[]{(byte) 0x00};
    writePayload(nullTerminator);
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
