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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.ConnectionStatus;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyDataReceiverTest {

  @Test
  public void testCopyStatementWithException() {
    SpannerException exception =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, "Invalid copy statement");
    CopyStatement statement = mock(CopyStatement.class);
    when(statement.hasException()).thenReturn(true);
    when(statement.getException()).thenReturn(exception);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);

    CopyDataReceiver receiver = new CopyDataReceiver(statement, connectionHandler);
    SpannerException spannerException = assertThrows(SpannerException.class, receiver::handleCopy);

    assertSame(exception, spannerException);
  }

  @Test
  public void testCopyStatementWithCopyFailed() {
    DataOutputStream outputStream = new DataOutputStream(new ByteArrayOutputStream());
    CopyStatement statement = mock(CopyStatement.class);
    ConnectionMetadata connectionMetadata = mock(ConnectionMetadata.class);
    when(connectionMetadata.peekOutputStream()).thenReturn(outputStream);
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getStatus())
        .thenReturn(ConnectionStatus.AUTHENTICATED, ConnectionStatus.COPY_FAILED);
    when(connectionHandler.getConnectionMetadata()).thenReturn(connectionMetadata);

    CopyDataReceiver receiver = new CopyDataReceiver(statement, connectionHandler);
    SpannerException spannerException = assertThrows(SpannerException.class, receiver::handleCopy);

    assertEquals(ErrorCode.INTERNAL, spannerException.getErrorCode());
    assertEquals("INTERNAL: Copy failed with unknown reason", spannerException.getMessage());
  }
}
