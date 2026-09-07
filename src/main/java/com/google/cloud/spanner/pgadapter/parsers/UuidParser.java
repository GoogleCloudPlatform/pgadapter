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
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.NullValue;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.postgresql.util.ByteConverter;

/** Translate from wire protocol to UUID. */
@InternalApi
public class UuidParser extends Parser<UUID> {
  private static final Value NULL_VALUE =
      Value.untyped(
          com.google.protobuf.Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());

  UuidParser(Object item) {
    this.item = (UUID) item;
  }

  UuidParser(byte[] item, FormatCode formatCode) {
    this.item = toUuid(item, formatCode);
  }

  /** Converts the given data to a UUID based on the format code. */
  public static UUID toUuid(byte[] item, FormatCode formatCode) {
    switch (formatCode) {
      case TEXT:
        return item == null ? null : verifyStringValue(new String(item, StandardCharsets.UTF_8));
      case BINARY:
        return verifyBinaryValue(item);
      default:
        handleInvalidFormat(formatCode);
        return null;
    }
  }

  UuidParser(ResultSet item, int position) {
    this.item = item.isNull(position) ? null : item.getUuid(position);
  }

  static void handleInvalidFormat(FormatCode formatCode) {
    throw PGException.newBuilder("Unsupported format: " + formatCode.name())
        .setSQLState(SQLState.InternalError)
        .setSeverity(Severity.ERROR)
        .build();
  }

  @Override
  public String stringParse() {
    return this.item == null ? null : this.item.toString();
  }

  @Override
  protected byte[] binaryParse() {
    if (this.item == null) {
      return null;
    }
    return binaryEncode(this.item);
  }

  static UUID verifyStringValue(@Nonnull String value) {
    try {
      return UUID.fromString(value);
    } catch (Exception exception) {
      throw createInvalidUuidValueException(value, exception);
    }
  }

  static UUID verifyBinaryValue(byte[] value) {
    if (value == null) {
      return null;
    }
    if (value.length != 16) {
      throw PGException.newBuilder("Invalid UUID binary length: " + value.length)
          .setSeverity(Severity.ERROR)
          .setSQLState(SQLState.InvalidParameterValue)
          .build();
    }
    return new UUID(ByteConverter.int8(value, 0), ByteConverter.int8(value, 8));
  }

  static byte[] binaryEncode(String value) {
    try {
      return binaryEncode(UUID.fromString(value));
    } catch (Exception exception) {
      throw createInvalidUuidValueException(value, exception);
    }
  }

  static byte[] binaryEncode(UUID uuid) {
    byte[] val = new byte[16];
    ByteConverter.int8(val, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(val, 8, uuid.getLeastSignificantBits());
    return val;
  }

  static PGException createInvalidUuidValueException(String value, Exception cause) {
    return PGException.newBuilder("Invalid UUID: " + value)
        .setSeverity(Severity.ERROR)
        .setSQLState(SQLState.InvalidParameterValue)
        .setCause(cause)
        .build();
  }

  public static byte[] convertToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    writeToPG(sessionState, outputStream, resultSet, position, format);
    return null;
  }

  static void writeToPG(
      @Nonnull SessionState sessionState,
      DataOutputStream outputStream,
      ResultSet resultSet,
      int position,
      DataFormat format)
      throws IOException {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        StringParser.writeToPG(sessionState, outputStream, getUuidAsString(resultSet, position));
        break;
      case POSTGRESQL_BINARY:
        UUID uuid = resultSet.getUuid(position);
        outputStream.writeInt(16);
        outputStream.writeLong(uuid.getMostSignificantBits());
        outputStream.writeLong(uuid.getLeastSignificantBits());
        break;
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  static String getUuidAsString(ResultSet resultSet, int column) {
    if (resultSet instanceof ProtobufResultSet
        && ((ProtobufResultSet) resultSet).canGetProtobufValue(column)) {
      return ((ProtobufResultSet) resultSet).getProtobufValue(column).getStringValue();
    }
    return resultSet.getUuid(column).toString();
  }

  public static byte[] convertToPG(ResultSet resultSet, int position, DataFormat format) {
    switch (format) {
      case SPANNER:
      case POSTGRESQL_TEXT:
        return resultSet.getUuid(position).toString().getBytes(StandardCharsets.UTF_8);
      case POSTGRESQL_BINARY:
        return binaryEncode(resultSet.getUuid(position));
      default:
        throw new IllegalArgumentException("unknown data format: " + format);
    }
  }

  public static void bind(
      ImmutableMap.Builder<String, Value> parametersBuilder,
      String name,
      byte[] item,
      FormatCode formatCode) {
    UUID uuid = toUuid(item, formatCode);
    parametersBuilder.put(
        name,
        uuid == null
            ? NULL_VALUE
            : Value.untyped(
                com.google.protobuf.Value.newBuilder().setStringValue(uuid.toString()).build()));
  }

  @Override
  public void bind(ImmutableMap.Builder<String, Value> parametersBuilder, String name) {
    // Send UUIDs to Spanner as untyped string values, so these can be used with both varchar and
    // UUID columns. This ensures backwards compatibility, as PGAdapter would send UUID values as
    // strings to Spanner before UUID type support was added to Spanner.
    parametersBuilder.put(
        name,
        this.item == null
            ? NULL_VALUE
            : Value.untyped(
                com.google.protobuf.Value.newBuilder()
                    .setStringValue(this.item.toString())
                    .build()));
  }
}
