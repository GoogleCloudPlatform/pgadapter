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
import java.util.List;

/**
 * Normally used to send a function call the back-end. Spanner does not currently support this, so
 * send will throw a descriptive error to be sent to the user. Note that we do parse this in order
 * for this to be future proof, and to ensure the input stream is flushed of the command (in order
 * to continue receiving properly)
 */
public class FunctionCallMessage extends WireMessage {

  private int functionID;
  private List<Short> argumentFormatCodes;
  private byte[][] arguments;
  private Short resultFormatCode;

  public FunctionCallMessage(ConnectionHandler connection, DataInputStream input) throws Exception {
    super(connection, input);
    this.functionID = input.readInt();
    this.argumentFormatCodes = getFormatCodes(input);
    this.arguments = getParameters(input);
    this.resultFormatCode = input.readShort();
  }

  @Override
  public void send() throws Exception {
    throw new IllegalStateException("Spanner does not currently support function calls.");
  }
}
