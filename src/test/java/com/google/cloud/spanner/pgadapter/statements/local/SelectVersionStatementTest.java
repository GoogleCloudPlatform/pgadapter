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

package com.google.cloud.spanner.pgadapter.statements.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.pgadapter.session.PGSetting;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectVersionStatementTest {
  @Test
  public void testExecute() {
    for (String version : new String[] {"14.1", "10.0"}) {
      BackendConnection backendConnection = mock(BackendConnection.class);
      SessionState sessionState = mock(SessionState.class);
      PGSetting pgSetting = mock(PGSetting.class);
      when(backendConnection.getSessionState()).thenReturn(sessionState);
      when(sessionState.get(null, "server_version")).thenReturn(pgSetting);
      when(pgSetting.getSetting()).thenReturn(version);

      try (ResultSet resultSet =
          SelectVersionStatement.INSTANCE.execute(backendConnection).getResultSet()) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getColumnCount());
        assertEquals("PostgreSQL " + version, resultSet.getString("version"));
        assertFalse(resultSet.next());
      }
    }
  }
}
