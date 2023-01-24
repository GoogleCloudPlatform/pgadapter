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

import com.google.cloud.spanner.myadapter.parsers.ParserHelper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class HeaderMessage {

  private int remainingPayloadLength;
  private int messageSequenceNumber = -1;
  private ByteArrayInputStream bufferedInputStream;
  private byte[] inputBuffer;

  public int getRemainingPayloadLength() {
    return remainingPayloadLength;
  }

  public int getMessageSequenceNumber() {
    return messageSequenceNumber;
  }

  public ByteArrayInputStream getBufferedInputStream() {
    return bufferedInputStream;
  }

  public static HeaderMessage create(DataInputStream inputStream) throws IOException {
    HeaderMessage headerMessage = new HeaderMessage();
    headerMessage.remainingPayloadLength =
        ParserHelper.parse3ByteInt(
            inputStream.readUnsignedByte(),
            inputStream.readUnsignedByte(),
            inputStream.readUnsignedByte());
    headerMessage.messageSequenceNumber = inputStream.readUnsignedByte();
    headerMessage.inputBuffer = new byte[headerMessage.remainingPayloadLength];
    inputStream.readFully(headerMessage.inputBuffer);
    headerMessage.bufferedInputStream = new ByteArrayInputStream(headerMessage.inputBuffer);
    return headerMessage;
  }
}
