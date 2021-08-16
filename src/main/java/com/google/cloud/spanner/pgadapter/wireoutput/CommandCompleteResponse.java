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

/** Signals to a client that an issued query is complete. */
public class CommandCompleteResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte NULL_TERMINATOR = 0;

  private final byte[] command;

  public CommandCompleteResponse(DataOutputStream output, String command) {
    super(output, HEADER_LENGTH + command.getBytes(UTF8).length + NULL_TERMINATOR_LENGTH);
    this.command = command.getBytes(UTF8);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.write(command);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.flush();
  }

  @Override
  public byte getIdentifier() {
    return 'C';
  }

  @Override
  protected String getMessageName() {
    return "Command Complete";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Command: {1}")
        .format(new Object[] {this.length, new String(this.command, UTF8)});
  }
}
