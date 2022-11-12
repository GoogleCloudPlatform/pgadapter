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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

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

  @Test
  public void testEquals() {
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.RaiseException)
            .build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.RaiseException)
            .build());
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.RaiseException)
            .build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.RaiseException)
            .build());
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build());
    assertEquals(
        PGException.newBuilder("test error").setSeverity(Severity.ERROR).build(),
        PGException.newBuilder("test error").setSeverity(Severity.ERROR).build());
    assertEquals(
        PGException.newBuilder("test error").build(), PGException.newBuilder("test error").build());

    assertNotEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build(),
        PGException.newBuilder("other test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build());
    assertNotEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.InternalError)
            .build());
    assertNotEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.RaiseException)
            .build());
    assertNotEquals(
        PGException.newBuilder("test error").setSeverity(Severity.ERROR).build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build());
    assertNotEquals(
        PGException.newBuilder("test error").build(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build());
  }

  @Test
  public void testHashCode() {
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.RaiseException)
            .build()
            .hashCode(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.RaiseException)
            .build()
            .hashCode());
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.RaiseException)
            .build()
            .hashCode(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.RaiseException)
            .build()
            .hashCode());
    assertEquals(
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build()
            .hashCode(),
        PGException.newBuilder("test error")
            .setSeverity(Severity.ERROR)
            .setSQLState(SQLState.InternalError)
            .build()
            .hashCode());
    assertEquals(
        PGException.newBuilder("test error").setSeverity(Severity.ERROR).build().hashCode(),
        PGException.newBuilder("test error").setSeverity(Severity.ERROR).build().hashCode());
    assertEquals(
        PGException.newBuilder("test error").build().hashCode(),
        PGException.newBuilder("test error").build().hashCode());
  }
}
