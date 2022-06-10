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
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

/** Sends error information back to client. */
@InternalApi
public class ErrorResponse extends WireOutput {
  public enum Severity {
    ERROR,
    FATAL,
    PANIC,
  }

  private static final byte[] SEVERITY = "ERROR".getBytes(UTF8);
  private static final byte[] SQL_STATE = "XX000".getBytes(UTF8);
  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_IDENTIFIER_LENGTH = 1;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte CODE_FLAG = 'C';
  private static final byte MESSAGE_FLAG = 'M';
  private static final byte SEVERITY_FLAG = 'S';
  private static final byte NULL_TERMINATOR = 0;

  private final byte[] severity;
  private final byte[] errorMessage;
  private final byte[] errorState; // TODO pull state from exception itself.

  public ErrorResponse(DataOutputStream output, Exception e, State errorState) {
    this(output, e, errorState, Severity.ERROR);
  }

  public ErrorResponse(DataOutputStream output, Exception e, State errorState, Severity severity) {
    super(
        output,
        HEADER_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + SEVERITY.length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + errorState.getBytes().length
            + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH
            + getMessageFromException(e).length
            + NULL_TERMINATOR_LENGTH
            + NULL_TERMINATOR_LENGTH);
    this.errorMessage = getMessageFromException(e);
    this.errorState = errorState.getBytes();
    this.severity = severity.name().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(SEVERITY_FLAG);
    this.outputStream.write(severity);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(CODE_FLAG);
    this.outputStream.write(this.errorState);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(MESSAGE_FLAG);
    this.outputStream.write(this.errorMessage);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(NULL_TERMINATOR);
  }

  @Override
  public byte getIdentifier() {
    return 'E';
  }

  @Override
  protected String getMessageName() {
    return "Error";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}, " + "Error Message: {1}")
        .format(new Object[] {this.length, new String(this.errorMessage, UTF8)});
  }

  private static byte[] getMessageFromException(Exception e) {
    return (e.getMessage() == null ? e.getClass().getName() : e.getMessage()).getBytes(UTF8);
  }

  public enum State {
    Success("00000"),
    Warning("01000"),
    DynamicResultSetReturned("0100C"),
    ImplicitZeroBitPadding("01008"),
    NullValueEliminatedInSetFunction("01003"),
    PrivilegeNotGranted("01007"),
    PrivilegeNotRevoked("01006"),
    StringDataRightTruncation("01004"),
    DeprecatedFeature("01P01"),
    NoData("02000"),
    NoAdditionalDynamicResultSetsReturned("02001"),
    SQLStatementNotYetComplete("03000"),
    ConnectionException("08000"),
    ConnectionDoesNotExist("08003"),
    ConnectionFailure("08006"),
    SQLClientUnableToEstablishSQLConnection("08001"),
    SQLServerRejectedEstablishmentOfSQLConnection("08004"),
    TransactionResultionUnknown("08007"),
    ProtocolViolation("08P01"),
    TriggeredActionException("09000"),
    FeatureNotSupported("0A000"),
    InvalidTransactionInitiation("0B000"),
    LocatorException("0F0000"),
    InvalidLocatorSpecification("0F100"),
    InvalidGrantor("0L000"),
    InvalidGrantOperation("0LP01"),
    InvalidRoleSpecification("0P000"),
    // Skipping Query exceptions for now as we cannot easily identify them here
    OperatorIntervention("57000"),
    QueryCanceled("57014"),
    AdminShutdown("57P01"),
    CrashShutdown("57P02"),
    CannotConnectNow("57P03"),
    IOError("58030"),
    UndefinedFile("58P01"),
    DuplicateFile("58P02"),
    ConfigFileError("F0000"),
    LockFileExists("F0001"),
    PLPGSQLError("P0000"),
    RaiseException("P0001"),
    NoDataFound("P0002"),
    TooManyRows("P0003"),
    InternalError("XX000"),
    DataCorrupted("XX001"),
    IndexCorrupted("XX002"),
    InvalidAuthorizationSpecification("28000"),
    InvalidPassword("28P01");

    private final String code;

    private State(String code) {
      this.code = code;
    }

    public boolean equals(String otherCode) {
      return code.equals(otherCode);
    }

    public String toString() {
      return this.code;
    }

    public byte[] getBytes() {
      return this.code.getBytes(UTF8);
    }
  }
}
