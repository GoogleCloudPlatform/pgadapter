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

import com.google.api.client.util.Preconditions;
import com.google.api.core.InternalApi;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * Signals readiness status to receive messages (here we only tend to send Idle, which means ready)
 */
@InternalApi
public class ReadyResponse extends WireOutput {
  //    IDLE('I'),
  //    TRANSACTION('T'),
  //    FAILED('E');
  private static final byte[] IDLE_RESPONSE = new byte[] {'Z', 0, 0, 0, 5, (byte) Status.IDLE.c};
  private static final byte[] TRANSACTION_RESPONSE =
      new byte[] {'Z', 0, 0, 0, 5, (byte) Status.TRANSACTION.c};
  private static final byte[] FAILED_RESPONSE =
      new byte[] {'Z', 0, 0, 0, 5, (byte) Status.FAILED.c};

  public static void sendReadyResponse(DataOutputStream output, Status status) throws IOException {
    switch (status) {
      case IDLE:
        sendIdleResponse(output);
        break;
      case TRANSACTION:
        sendTransactionResponse(output);
        break;
      case FAILED:
        sendFailedResponse(output);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static void sendIdleResponse(DataOutputStream output) throws IOException {
    output.write(IDLE_RESPONSE);
  }

  public static void sendTransactionResponse(DataOutputStream output) throws IOException {
    output.write(TRANSACTION_RESPONSE);
  }

  public static void sendFailedResponse(DataOutputStream output) throws IOException {
    output.write(FAILED_RESPONSE);
  }

  private final Status status;

  public ReadyResponse(DataOutputStream output, Status status) {
    super(output, 5);
    this.status = Preconditions.checkNotNull(status);
  }

  @Override
  public void sendPayload() throws IOException {
    this.outputStream.writeByte(this.status.c);
  }

  @Override
  public byte getIdentifier() {
    return 'Z';
  }

  @Override
  protected String getMessageName() {
    return "Ready";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Status: {1}")
        .format(new Object[] {this.length, this.status.c});
  }

  /** Status of the session. */
  public enum Status {
    IDLE('I'),
    TRANSACTION('T'),
    FAILED('E');
    private final char c;

    Status(char c) {
      this.c = c;
    }
  }
}
