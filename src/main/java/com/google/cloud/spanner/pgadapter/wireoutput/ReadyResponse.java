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

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * Signals readiness status to receieve messages (here we only tend to send Idle, which means ready)
 */
public class ReadyResponse extends WireOutput {

  private final Status status;

  public ReadyResponse(DataOutputStream output, Status status) {
    super(output, 5);
    this.status = status;
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.writeByte(this.status.c);
  }

  @Override
  public byte getIdentifier() {
    return 'Z';
  }

  @Override
  protected String getMessageName() {
    return "Ready";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Status: {1}")
        .format(new Object[] {this.length, this.status.c});
  }

  /** Status of the session. */
  public enum Status {
    IDLE('I'),
    TRANSACTION('T'),
    FAILED('E');
    private final char c;

    Status(char c) {
      this.c = c;
    }
  }
}
