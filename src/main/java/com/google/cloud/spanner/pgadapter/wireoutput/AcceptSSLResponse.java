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

import com.google.api.core.InternalApi;
import java.io.DataOutputStream;
import java.io.IOException;

/** Signals to client that we accept SSL. */
@InternalApi
public class AcceptSSLResponse extends WireOutput {

  public AcceptSSLResponse(DataOutputStream output) {
    super(output, 4);
  }

  @Override
  public void sendPayload() throws IOException {}

  @Override
  protected boolean isCompoundResponse() {
    return false;
  }

  @Override
  public byte getIdentifier() {
    return 'S';
  }

  @Override
  protected String getMessageName() {
    return "Accept SSL";
  }

  @Override
  protected String getPayloadString() {
    return "";
  }
}
