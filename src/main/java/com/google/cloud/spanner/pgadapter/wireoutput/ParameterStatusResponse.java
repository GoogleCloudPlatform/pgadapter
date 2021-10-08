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

/** Sends back start-up parameters (can be sent more than once) */
public class ParameterStatusResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte NULL_TERMINATOR = 0;

  public final byte[] parameterKey;
  public final byte[] parameterValue;

  public ParameterStatusResponse(
      DataOutputStream output, byte[] parameterKey, byte[] parameterValue) {
    super(
        output,
        HEADER_LENGTH
            + parameterKey.length
            + NULL_TERMINATOR_LENGTH
            + parameterValue.length
            + NULL_TERMINATOR_LENGTH);
    this.parameterKey = parameterKey;
    this.parameterValue = parameterValue;
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.write(this.parameterKey);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.write(this.parameterValue);
    this.outputStream.writeByte(NULL_TERMINATOR);
  }

  @Override
  public byte getIdentifier() {
    return 'S';
  }

  @Override
  protected String getMessageName() {
    return "Parameter Status";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Parameter Key: {1}, " + "Parameter Value: {2}")
        .format(
            new Object[] {
              this.length,
              new String(this.parameterKey, UTF8),
              new String(this.parameterValue, UTF8)
            });
  }
}
