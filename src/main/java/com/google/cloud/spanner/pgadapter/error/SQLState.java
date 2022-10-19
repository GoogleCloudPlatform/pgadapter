// Copyright 2022 Google LLC
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

package com.google.cloud.spanner.pgadapter.error;

import java.nio.charset.StandardCharsets;

public enum SQLState {
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
  InvalidPassword("28P01"),

  // Class 42 — Syntax Error or Access Rule Violation
  SyntaxErrorOrAccessRuleViolation("42000"),
  SyntaxError("42601"),
  InsufficientPrivilege("42501"),
  CannotCoerce("42846"),
  GroupingError("42803"),
  WindowingError("42P20"),
  InvalidRecursion("42P19"),
  InvalidForeignKey("42830"),
  InvalidName("42602"),
  NameTooLong("42622"),
  ReservedName("42939"),
  DatatypeMismatch("42804"),
  IndeterminateDatatype("42P18"),
  CollationMismatch("42P21"),
  IndeterminateCollation("42P22"),
  WrongOjectType("42809"),
  GeneratedAlways("428C9"),
  UndefinedColumn("42703"),
  UndefinedFunction("42883"),
  UndefinedTable("42P01"),
  UndefinedParameter("42P02"),
  UndefinedObject("42704"),
  DuplicateColumn("42701"),
  DuplicateCursor("42P03"),
  DuplicateDatabase("42P04"),
  DuplicateFunction("42723"),
  DuplicatePreparedStatement("42P05"),
  DuplicateSchema("42P06"),
  DuplicateTable("42P07"),
  DuplicateAlias("42712"),
  DuplicateObject("42710"),
  AmbiguousColumn("42702"),
  AmbiguousFunction("42725"),
  AmbiguousParameter("42P08"),
  AmbiguousAlias("42P09"),
  InvalidColumnReference("42P10"),
  InvalidColumnDefinition("42611"),
  InvalidCursorDefinition("42P11"),
  InvalidDatabaseDefinition("42P12"),
  InvalidFunctionDefinition("42P13"),
  InvalidPreparedStatementDefinition("42P14"),
  InvalidSchemaDefinition("42P15"),
  InvalidTableDefinition("42P16"),
  InvalidObjectDefinition("42P17");

  private final String code;

  SQLState(String code) {
    this.code = code;
  }

  public boolean equals(String otherCode) {
    return code.equals(otherCode);
  }

  public String toString() {
    return this.code;
  }

  public byte[] getBytes() {
    return this.code.getBytes(StandardCharsets.UTF_8);
  }
}
