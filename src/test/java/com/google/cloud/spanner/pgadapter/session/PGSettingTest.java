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

package com.google.cloud.spanner.pgadapter.session;

import static com.google.cloud.spanner.pgadapter.session.PGSetting.tryParseContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.session.PGSetting.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PGSettingTest {

  @Test
  public void testTryParseContext() {
    assertEquals(Context.INTERNAL, tryParseContext("internal"));
    assertEquals(Context.SUPERUSER_BACKEND, tryParseContext("superuser-backend"));
    assertEquals(Context.USER, tryParseContext("USER"));
    assertEquals(Context.SUPERUSER, tryParseContext("superUSER"));
    assertEquals(Context.POSTMASTER, tryParseContext("Postmaster"));

    assertEquals(Context.USER, tryParseContext("random-value"));
    assertEquals(Context.USER, tryParseContext(""));
    assertEquals(Context.USER, tryParseContext(null));
  }

  @Test
  public void testIsSettable() {
    assertTrue(SessionState.SERVER_SETTINGS.get("server_version").isSettable(Context.INTERNAL));
    assertTrue(SessionState.SERVER_SETTINGS.get("server_version").isSettable(Context.BACKEND));
    assertFalse(SessionState.SERVER_SETTINGS.get("server_version").isSettable(Context.SUPERUSER));
  }
}
