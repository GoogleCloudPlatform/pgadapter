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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import java.io.IOException;
import java.text.MessageFormat;

public class SkipMessage extends ControlMessage {

  public SkipMessage(ConnectionHandler connectionHandler) throws IOException {
    super(connectionHandler);
    int skipLength = this.length - 4;
    int skipped = 0;
    while (skipped < skipLength) {
      skipped += connection.getConnectionMetadata().getInputStream().skip(skipLength - skipped);
    }
  }

  @Override
  protected void sendPayload() throws Exception {}

  @Override
  protected String getMessageName() {
    return "Skip";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }

  @Override
  protected String getIdentifier() {
    return "";
  }
}
