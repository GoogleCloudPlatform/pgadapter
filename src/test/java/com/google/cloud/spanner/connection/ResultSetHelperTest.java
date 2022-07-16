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

package com.google.cloud.spanner.connection;

import static com.google.cloud.spanner.connection.ResultSetHelper.toDirectExecuteResultSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ResultSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResultSetHelperTest {

  @Test
  public void testToDirectExecuteResultSet() {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true, true, false);

    ResultSet directExecuteResultSet = toDirectExecuteResultSet(resultSet);

    // The underlying result set should call next() to execute the query.
    verify(resultSet).next();

    // The returned result set should have 2 rows.
    assertTrue(directExecuteResultSet.next());
    assertTrue(directExecuteResultSet.next());
    assertFalse(directExecuteResultSet.next());
  }
}
