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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGExceptionFactory;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.postgresql.util.PGbytea;

/**
 * Parse specified type to binary (generally this is the simplest parse class, as items are
 * generally represented in binary for wire format).
 */
@InternalApi
public class BinaryParser extends Parser<ByteArray> {

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
    return this.item == null ? null : bytesToHex(this.item.toByteArray());
  }

  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2 + 2];
    hexChars[0] = '\\';
    hexChars[1] = 'x';
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2 + 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 3] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  @Override
  protected byte[] spannerBinaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  @Override
  protected byte[] binaryParse() {
    return this.item == null ? null : this.item.toByteArray();
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_BINARY:
        return resultSet.getBytes(position).toByteArray();
      case POSTGRESQL_TEXT:
        return (bytesToHex(resultSet.getBytes(position).toByteArray()))
            .getBytes(StandardCharsets.UTF_8);
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
