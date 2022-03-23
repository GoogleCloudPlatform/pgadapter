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
import java.util.Arrays;

/** Response (with data) for describe statement. */
public class ParameterDescriptionResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int PARAMETER_NUMBER_LENGTH = 2;
  private static final int PARAMETER_LENGTH = 4;

  private final int[] parameters;

  public ParameterDescriptionResponse(DataOutputStream output, int[] parameters) {
    super(output, calculateLength(parameters));
    this.parameters = parameters;
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeShort(this.parameters.length);
    for (int parameter : this.parameters) {
      this.outputStream.writeInt(parameter);
    }
  }

  @Override
  public byte getIdentifier() {
    return 't';
  }

  @Override
  protected String getMessageName() {
    return "Parameter Description";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Parameters: {1}")
        .format(new Object[] {this.length, Arrays.toString(this.parameters)});
  }

  private static int calculateLength(int[] parameters) {
    return HEADER_LENGTH + PARAMETER_NUMBER_LENGTH + (parameters.length * PARAMETER_LENGTH);
  }
}
