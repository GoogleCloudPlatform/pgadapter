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

package com.google.cloud.spanner.pgadapter.statements;

import static com.google.cloud.spanner.pgadapter.statements.BackendConnection.extractDdlUpdateCounts;
import static org.junit.Assert.assertArrayEquals;

import com.google.cloud.spanner.pgadapter.statements.BackendConnection.NoResult;
import com.google.cloud.spanner.pgadapter.statements.DdlExecutor.NotExecuted;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BackendConnectionTest {
  private static final NotExecuted NOT_EXECUTED = new NotExecuted();
  private static final NoResult NO_RESULT = new NoResult();

  @Test
  public void testExtractDdlUpdateCounts() {
    assertArrayEquals(new long[] {}, extractDdlUpdateCounts(ImmutableList.of(), new long[] {}));
    assertArrayEquals(
        new long[] {1L}, extractDdlUpdateCounts(ImmutableList.of(NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(ImmutableList.of(NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(ImmutableList.of(NOT_EXECUTED, NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NO_RESULT), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NO_RESULT, NOT_EXECUTED),
            new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NO_RESULT, NOT_EXECUTED, NO_RESULT),
            new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NO_RESULT, NO_RESULT, NOT_EXECUTED), new long[] {1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L, 1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(
                NOT_EXECUTED,
                NO_RESULT,
                NO_RESULT,
                NOT_EXECUTED,
                NOT_EXECUTED,
                NO_RESULT,
                NO_RESULT),
            new long[] {1L, 1L, 1L}));
    assertArrayEquals(
        new long[] {1L, 1L, 1L},
        extractDdlUpdateCounts(
            ImmutableList.of(NOT_EXECUTED, NOT_EXECUTED, NOT_EXECUTED), new long[] {}));
  }
}
