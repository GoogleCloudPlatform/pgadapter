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

package com.google.cloud.spanner.pgadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/** Translate from wire protocol to string. */
@InternalApi
public class StringParser extends Parser<String> {
  private static final byte[] HEADER = new byte[0];

  StringParser(ResultSet item, int position) {
    this.item = getString(item, position);
  }

  StringParser(Object item) {
    this.item = (String) item;
  }

  StringParser(byte[] item, FormatCode ignore) {
    if (item != null) {
      this.item = toString(item);
    }
  }

  /** Converts the binary data to an UTF8 string. */
  public static String toString(@Nonnull byte[] data) {
    return new String(data, UTF8);
  }

  @Override
  public String stringParse() {
    return this.item;
  }

  @Override
  protected byte[] binaryParse() {
    return this.item == null ? null : this.item.getBytes(StandardCharsets.UTF_8);
  }

  /** Get the string from the result as efficiently as possible. */
  static String getString(ResultSet resultSet, int column) {
    // If the result set is a ProtobufResultSet and the protobuf value is still present, then get
    // the string directly from that.
    if (resultSet instanceof ProtobufResultSet
        && ((ProtobufResultSet) resultSet).canGetProtobufValue(column)) {
      return ((ProtobufResultSet) resultSet).getProtobufValue(column).getStringValue();
    } else {
      return resultSet.getString(column);
    }
  }

  public static byte[] convertToPG(
      SessionState sessionState,
      DataOutputStream dataOutputStream,
      ResultSet resultSet,
      int position) {
    writeToPG(sessionState, dataOutputStream, getString(resultSet, position));
    return null;
  }

  static void writeToPG(
      SessionState sessionState, DataOutputStream dataOutputStream, String value) {
    writeToPG(sessionState, dataOutputStream, value, HEADER);
  }

  static void writeToPG(
      SessionState sessionState, DataOutputStream dataOutputStream, String value, byte[] header) {
    int bufferSize = sessionState.getStringConversionBufferSize();
    // Skip getting the length if we don't need it.
    int length = bufferSize <= 0 ? 0 : value.length();
    try {
      if (bufferSize <= 0 || length < bufferSize) {
        // Just use the writeUTF method of DataOutputStream if there is no header that needs to be
        // written.
        if (header.length == 0) {
          // PG expects the length be 4 bytes.
          // writeUTF writes the length as 2 bytes.
          // So in order to get the correct length written to the stream, we first write 2 bytes
          // containing all zeros.
          dataOutputStream.writeShort(0);
          dataOutputStream.writeUTF(value);
        } else {
          byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
          dataOutputStream.writeInt(bytes.length + header.length);
          dataOutputStream.write(header);
          dataOutputStream.write(bytes);
        }
      } else {
        try (OutputStreamWriter writer =
            new OutputStreamWriter(dataOutputStream, StandardCharsets.UTF_8)) {
          int utf8Length = Utf8.encodedLength(value) + header.length;
          dataOutputStream.writeInt(utf8Length);
          dataOutputStream.write(header);
          for (int offset = 0; offset < length; offset += bufferSize) {
            int writeLen = Math.min(bufferSize, length - offset);
            writer.write(value, offset, writeLen);
          }
        }
      }
    } catch (IOException ioException) {
      throw SpannerExceptionFactory.asSpannerException(ioException);
    }
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    parametersBuilder.put(name, Value.string(this.item));
  }
}
