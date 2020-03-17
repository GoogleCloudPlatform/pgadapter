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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import java.io.DataInputStream;

/**
 * Normally used to signal a copy command failed. Spanner does not currently support copies, so send
 * will throw a descriptive error to be sent to the user. Note that we do parse this in order for
 * this to be future proof, and to ensure the input stream is flushed of the command (in order to
 * continue receiving properly)
 */
public class CopyFailMessage extends WireMessage {

  private String errorMessage;

  public CopyFailMessage(ConnectionHandler connection, DataInputStream input) throws Exception {
    super(connection, input);
    this.remainder = 4;
    this.errorMessage = this.read(input);
  }

  @Override
  public void send() throws Exception {
    throw new IllegalStateException(
        "Spanner does not currently support the copy functionality through the proxy.");
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }
}
