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

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Spanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConnectionOptionsHelperTest {

  public static Connection setupMockConnection(Spanner spanner) {
    ConnectionImpl connection = mock(ConnectionImpl.class);
    when(connection.getSpanner()).thenReturn(spanner);

    return connection;
  }

  @Test
  public void testSetCredentials() {
    ConnectionOptions.Builder builder = mock(ConnectionOptions.Builder.class);
    GoogleCredentials credentials = mock(GoogleCredentials.class);

    ConnectionOptionsHelper.setCredentials(builder, credentials);

    verify(builder).setCredentials(credentials);
  }

  @Test
  public void testGetSpanner() {
    ConnectionImpl connection = mock(ConnectionImpl.class);
    Spanner expected = mock(Spanner.class);
    when(connection.getSpanner()).thenReturn(expected);

    Spanner got = ConnectionOptionsHelper.getSpanner(connection);

    assertSame(expected, got);
  }
}
