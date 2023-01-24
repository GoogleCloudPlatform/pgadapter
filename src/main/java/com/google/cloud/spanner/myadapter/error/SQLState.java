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

package com.google.cloud.spanner.myadapter.error;

import java.nio.charset.StandardCharsets;

public enum SQLState {
  // TODO: This needs to be updated for MySQL.
  SQLServerRejectedEstablishmentOfSQLConnection("08004"),
  // Skipping Query exceptions for now as we cannot easily identify them here
  QueryCanceled("57014"),
  RaiseException("P0001"),
  InternalError("XX000"),

  // Class 42 — Syntax Error or Access Rule Violation
  SyntaxError("42601");

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
