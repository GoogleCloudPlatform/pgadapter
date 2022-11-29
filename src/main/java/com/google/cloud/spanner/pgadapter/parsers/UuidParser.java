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
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/**
 * Translate from wire protocol to UUID. This is currently a one-way conversion, as we only accept
 * UUID as a parameter type. UUIDs are converted to strings.
 */
@InternalApi
public class UuidParser extends Parser<String> {

  UuidParser(byte[] item, FormatCode formatCode) {
    switch (formatCode) {
      case TEXT:
        this.item =
            item == null ? null : verifyStringValue(new String(item, StandardCharsets.UTF_8));
        break;
      case BINARY:
        this.item = verifyBinaryValue(item);
        break;
      default:
        handleInvalidFormat(formatCode);
    }
  }

  static void handleInvalidFormat(FormatCode formatCode) {
    throw PGException.newBuilder("Unsupported format: " + formatCode.name())
        .setSQLState(SQLState.InternalError)
        .setSeverity(Severity.ERROR)
        .build();
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

  static String verifyStringValue(@Nonnull String value) {
    try {
      //noinspection ResultOfMethodCallIgnored
      UUID.fromString(value);
      return value;
    } catch (Exception exception) {
      throw createInvalidUuidValueException(value, exception);
    }
  }

  static String verifyBinaryValue(byte[] value) {
    if (value == null) {
      return null;
    }
    if (value.length != 16) {
      throw PGException.newBuilder("Invalid UUID binary length: " + value.length)
          .setSeverity(Severity.ERROR)
          .setSQLState(SQLState.InvalidParameterValue)
          .build();
    }
    return new UUID(ByteConverter.int8(value, 0), ByteConverter.int8(value, 8)).toString();
  }

  static byte[] binaryEncode(String value) {
    try {
      UUID uuid = UUID.fromString(value);
      byte[] val = new byte[16];
      ByteConverter.int8(val, 0, uuid.getMostSignificantBits());
      ByteConverter.int8(val, 8, uuid.getLeastSignificantBits());
      return val;
    } catch (Exception exception) {
      throw createInvalidUuidValueException(value, exception);
    }
  }

  static PGException createInvalidUuidValueException(String value, Exception cause) {
    return PGException.newBuilder("Invalid UUID: " + value)
        .setSeverity(Severity.ERROR)
        .setSQLState(SQLState.InvalidParameterValue)
        .setCause(cause)
        .build();
  }

  @Override
  public void bind(Statement.Builder statementBuilder, String name) {
    statementBuilder.bind(name).to(this.item);
  }
}
