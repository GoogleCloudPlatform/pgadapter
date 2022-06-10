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

package com.google.cloud.spanner.pgadapter.wireoutput;

import com.google.api.core.InternalApi;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;

@InternalApi
public class AuthenticationCleartextPasswordResponse extends WireOutput {
  private static final int CLEAR_TEXT_PASSWORD_REQUIRED_FLAG = 3;

  public AuthenticationCleartextPasswordResponse(DataOutputStream output) {
    super(output, 8);
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.writeInt(CLEAR_TEXT_PASSWORD_REQUIRED_FLAG);
  }

  @Override
  public byte getIdentifier() {
    return 'R';
  }

  @Override
  protected String getMessageName() {
    return "Clear-text Password Authentication Request";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[] {this.length});
  }
}
