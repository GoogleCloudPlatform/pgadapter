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
import com.google.cloud.spanner.pgadapter.statements.BackendConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SelectCurrentSchemaStatementTest {

  @Test
  public void testExecute() {
    for (String schema : new String[] {"public", "information_schema", "foo"}) {
      BackendConnection backendConnection = mock(BackendConnection.class);
      when(backendConnection.getCurrentSchema()).thenReturn(schema);

      try (ResultSet resultSet =
          SelectCurrentSchemaStatement.INSTANCE.execute(backendConnection).getResultSet()) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getColumnCount());
        assertEquals(schema, resultSet.getString("current_schema"));
        assertFalse(resultSet.next());
      }
    }
  }
}
