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
import com.google.cloud.spanner.pgadapter.utils.Converter;
import java.io.DataOutputStream;

/** Sends to the client specific row contents. */
@InternalApi
public class DataRowResponse extends WireOutput {
  private static final int HEADER_LENGTH = 4;

  private final Converter converter;

  public DataRowResponse(DataOutputStream output, Converter converter) {
    super(output, 0);
    this.converter = converter;
  }

  public void send(boolean flush) throws Exception {
    this.length = HEADER_LENGTH + converter.convertResultSetRowToDataRowResponse();
    super.send(flush);
  }

  @Override
  protected void sendPayload() throws Exception {
    converter.writeBuffer(this.outputStream);
  }

  @Override
  public byte getIdentifier() {
    return 'D';
  }

  @Override
  protected String getMessageName() {
    return "Data Row";
  }

  @Override
  protected String getPayloadString() {
    return "<REDACTED DUE TO LENGTH & PERFORMANCE CONSTRAINTS>";
  }
}
