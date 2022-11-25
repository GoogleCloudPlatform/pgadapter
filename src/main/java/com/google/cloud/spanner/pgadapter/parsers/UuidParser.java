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
import com.google.cloud.spanner.Statement;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to UUID. */
@InternalApi
public class UuidParser extends Parser<String> {

  UuidParser(byte[] item, FormatCode formatCode) {
    switch (formatCode) {
      case TEXT:
        this.item = new String(item, StandardCharsets.UTF_8);
        break;
      case BINARY:
        this.item = new UUID(ByteConverter.int8(item, 0), ByteConverter.int8(item, 8)).toString();
        break;
      default:
        throw new IllegalArgumentException("Unsupported format: " + formatCode);
    }
  }

  @Override
  public String stringParse() {
    return this.item;
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return binaryEncode(this.item);
  }

  static byte[] binaryEncode(String value) {
    UUID uuid = UUID.fromString(value);
    byte[] val = new byte[16];
    ByteConverter.int8(val, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(val, 8, uuid.getLeastSignificantBits());
    return val;
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
