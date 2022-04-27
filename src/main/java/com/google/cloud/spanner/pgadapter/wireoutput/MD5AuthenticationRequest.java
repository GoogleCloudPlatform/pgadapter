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
import java.text.MessageFormat;
import org.postgresql.util.ByteConverter;

/** Used for password response. */
@InternalApi
public class MD5AuthenticationRequest extends WireOutput {

  private static final int PASSWORD_REQUIRED_FLAG = 5;

  private byte[] salt = new byte[4];

  public MD5AuthenticationRequest(DataOutputStream output, int salt) {
    super(output, 12);
    ByteConverter.int4(this.salt, 0, salt);
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.writeInt(PASSWORD_REQUIRED_FLAG);
    this.outputStream.write(this.salt);
  }

  @Override
  public byte getIdentifier() {
    return 'R';
  }

  @Override
  protected String getMessageName() {
    return "MD5 Authentication Request";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Salt: {1}")
        .format(new Object[] {this.length, this.salt});
  }
}
