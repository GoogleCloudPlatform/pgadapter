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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * {@link SkipMessage} is a pseudo wire-protocol message that is used to read and skip messages that
 * we receive that we do not want at that time. This applies to COPY messages during normal
 * operation, and sync/flush messages during COPY operation.
 */
@InternalApi
public class SkipMessage extends ControlMessage {

  public static SkipMessage createForValidStream(ConnectionHandler connectionHandler)
      throws IOException {
    return new SkipMessage(connectionHandler, false);
  }

  public static SkipMessage createForInvalidStream(ConnectionHandler connectionHandler)
      throws IOException {
    return new SkipMessage(connectionHandler, true);
  }

  private SkipMessage(ConnectionHandler connectionHandler, boolean streamIsPotentiallyInvalid)
      throws IOException {
    super(connectionHandler);
    int skipLength = this.length - 4;
    int skipped = 0;
    // Read and skip bytes until we have reached the total message length.
    DataInputStream input = connection.getConnectionMetadata().peekInputStream();
    // If the stream is potentially invalid we'll use a more pessimistic skipping algorithm than if
    // we just received a message that we want to skip. We do this because the Unix domain socket
    // implementation might block on skip(n) if the stream has no bytes more to read.
    if (streamIsPotentiallyInvalid) {
      skipInvalid(input, skipLength);
    } else {
      while (skipped < skipLength) {
        skipped += input.skip(skipLength - skipped);
      }
    }
  }

  private void skipInvalid(DataInputStream input, int numBytes) throws IOException {
    int skipped = 0;
    while (skipped < numBytes && input.read() != -1) {
      skipped++;
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
