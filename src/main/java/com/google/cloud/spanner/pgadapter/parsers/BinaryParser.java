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
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import com.google.common.io.CharSource;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.annotation.Nonnull;
import org.postgresql.util.PGbytea;

/**
 * Parse specified type to binary (generally this is the simplest parse class, as items are
 * generally represented in binary for wire format).
 */
@InternalApi
public class BinaryParser extends Parser<ByteArray> {
  private static final int MAX_BUFFER_SIZE = 1 << 15;

  BinaryParser(ResultSet item, int position) {
    this.item = item.getBytes(position);
  }

  BinaryParser(Object item) {
    this.item = (ByteArray) item;
  }

  BinaryParser(byte[] item, FormatCode formatCode) {
    if (item != null) {
      switch (formatCode) {
        case TEXT:
          try {
            this.item = ByteArray.copyFrom(PGbytea.toBytes(item));
            break;
          } catch (Exception exception) {
            throw PGExceptionFactory.newPGException(
                "Invalid binary value: " + new String(item, StandardCharsets.UTF_8));
          }
        case BINARY:
          this.item = toByteArray(item);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + formatCode);
      }
    }
  }

  /** Converts the binary data to a {@link ByteArray}. */
  public static ByteArray toByteArray(@Nonnull byte[] data) {
    return ByteArray.copyFrom(data);
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : new String(bytesToHex(this.item.toByteArray()));
  }

  private static final byte[] HEX_ARRAY = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

  static byte[] bytesToHex(byte[] bytes) {
    byte[] hexChars = new byte[bytes.length * 2 + 2];
    hexChars[0] = '\\';
    hexChars[1] = 'x';
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2 + 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 3] = HEX_ARRAY[v & 0x0F];
    }
    return hexChars;
  }

  @Override
  protected byte[] spannerBinaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  @Override
  protected byte[] binaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  public static byte[] convertToPG(
      DataOutputStream dataOutputStream, ResultSet resultSet, int position, DataFormat format) {
    try {
      String base64 = resultSet.getValue(position).getAsString();
      switch (format) {
        case SPANNER:
        case POSTGRESQL_BINARY:
          int length = base64ByteLength(base64);
          dataOutputStream.writeInt(length);
          if (length > MAX_BUFFER_SIZE) {
            try (InputStream inputStream =
                Base64.getDecoder()
                    .wrap(
                        CharSource.wrap(base64)
                            .asByteSource(StandardCharsets.ISO_8859_1)
                            .openStream())) {
              copy(length, inputStream, dataOutputStream);
            }
          } else {
            dataOutputStream.write(Base64.getDecoder().decode(base64));
          }
          return null;
        case POSTGRESQL_TEXT:
          return bytesToHex(resultSet.getBytes(position).toByteArray());
        default:
          throw new IllegalArgumentException("unknown data format: " + format);
      }
    } catch (IOException ioException) {
      throw SpannerExceptionFactory.asSpannerException(ioException);
    }
  }

  static long copy(int length, InputStream from, DataOutputStream to) throws IOException {
    byte[] buf = new byte[Math.min(length, MAX_BUFFER_SIZE)];
    long total = 0;
    int numRead;
    while ((numRead = from.read(buf)) > -1) {
      to.write(buf, 0, numRead);
      total += numRead;
    }
    return total;
  }

  static int base64ByteLength(String base64) {
    int padding = 0;
    int index = base64.length();
    while (--index >= 0 && base64.charAt(index) == '=') {
      padding++;
    }
    int length = 3 * ((base64.length() - padding) / 4);
    if (padding == 2) {
      length += 1;
    } else if (padding == 1) {
      length += 2;
    }
    return length;
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
