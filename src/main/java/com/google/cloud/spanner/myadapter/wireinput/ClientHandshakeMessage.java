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

package com.google.cloud.spanner.myadapter.wireinput;

import com.google.api.core.InternalApi;
import java.text.MessageFormat;

/** Executes a simple statement. */
@InternalApi
public class ClientHandshakeMessage extends WireMessage {

  protected static final char IDENTIFIER = 'Q';

  public ClientHandshakeMessage(HeaderMessage headerMessage) throws Exception {
    super(headerMessage);
    // We don't need to do anything with the received payload at this point.
  }

  @Override
  protected void processRequest() throws Exception {}

  @Override
  protected String getMessageName() {
    return "ClientHandshake";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, SQL: {1}").format(new Object[] {this.length});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }
}
