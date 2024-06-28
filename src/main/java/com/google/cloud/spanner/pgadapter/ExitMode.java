// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter;

public enum ExitMode {
  /** Halts the JVM with exit code 0 if the shutdown was successful. */
  HALT_WITH_EXIT_CODE_ZERO_ON_SUCCESS,

  /**
   * Uses the default JVM exit code. This is equal to 128 + signal code if the server was halted by
   * a signal (e.g. ctrl-c or kill -2), or the exit code passed to System.exit(code).
   */
  USE_JVM_EXIT_CODE,
}
