// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.InternalApi;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Utility class for printing pure ASCII strings directly to a stream without allocating byte[].
 *
 * <p><strong>WARNING:</strong> This class should ONLY be used if the caller can guarantee that the
 * string will only contain ASCII characters (e.g., such as with numbers). It directly casts chars
 * to bytes, which will corrupt any non-ASCII characters.
 */
@InternalApi
public class AsciiPrinter {
  private static final ThreadLocal<byte[]> BUFFER = ThreadLocal.withInitial(() -> new byte[64]);

  /**
   * Writes the given ASCII string to the output stream.
   *
   * @param outputStream the stream to write to
   * @param value the string to write (must be ASCII-only)
   * @throws IOException if an I/O error occurs
   */
  public static void writeAsciiToPG(DataOutputStream outputStream, String value)
      throws IOException {
    int length = value.length();
    outputStream.writeInt(length);
    if (length > 0) {
      byte[] buffer = BUFFER.get();
      if (length > buffer.length) {
        buffer = new byte[length];
        BUFFER.set(buffer);
      }
      for (int i = 0; i < length; i++) {
        buffer[i] = (byte) value.charAt(i);
      }
      outputStream.write(buffer, 0, length);
    }
  }
}
