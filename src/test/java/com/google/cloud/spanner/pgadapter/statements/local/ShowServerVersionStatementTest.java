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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShowServerVersionStatementTest {

  @Test
  public void testExecute() {
    for (String version : new String[] {"1.0.0", "14.0.1"}) {
      BackendConnection backendConnection = mock(BackendConnection.class);
      OptionsMetadata options = mock(OptionsMetadata.class);
      when(options.getServerVersion()).thenReturn(version);
      when(backendConnection.getOptionsMetadata()).thenReturn(options);

      try (ResultSet resultSet =
          ShowServerVersionStatement.INSTANCE.execute(backendConnection).getResultSet()) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getColumnCount());
        assertEquals(version, resultSet.getString("server_version"));
        assertFalse(resultSet.next());
      }
    }
  }
}
