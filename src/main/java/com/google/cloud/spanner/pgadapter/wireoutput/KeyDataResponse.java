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
 * Sends back information regarding the current connection (identifier and secret) as part of
 * start-up. Used for cancel statements.
 */
public class KeyDataResponse extends WireOutput {

  private final int processId;
  private final int secretKey;

  public KeyDataResponse(DataOutputStream output, int processId, int secretKey) {
    super(output, 12);
    this.processId = processId;
    this.secretKey = secretKey;
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.writeInt(this.processId);
    this.outputStream.writeInt(this.secretKey);
  }

  @Override
  public byte getIdentifier() {
    return 'K';
  }

  @Override
  protected String getMessageName() {
    return "Key Data Information";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, "
            + "Process ID: {1}, "
            + "Secret Key: {2}")
        .format(new Object[]{
            this.length,
            this.processId,
            this.secretKey
        });
  }
}
