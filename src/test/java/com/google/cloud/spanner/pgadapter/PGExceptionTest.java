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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.error.Severity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PGExceptionTest {

  @Test
  public void testHints() {
    assertNull(
        PGException.newBuilder("test message")
            .setSQLState(SQLState.InternalError)
            .setSeverity(Severity.ERROR)
            .build()
            .getHints());
    assertEquals(
        "test hint\nsecond line",
        PGException.newBuilder("test message")
            .setSQLState(SQLState.InternalError)
            .setSeverity(Severity.ERROR)
            .setHints("test hint\nsecond line")
            .build()
            .getHints());
  }
}
