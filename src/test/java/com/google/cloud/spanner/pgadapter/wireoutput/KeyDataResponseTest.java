// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.wireoutput;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KeyDataResponseTest {

  @Test
  public void testSendAndRedactedLogging() throws Exception {
    byte[] secret = new byte[] {42, 73, 91, 107};
    int processId = 12345;
    String formattedProcessId = MessageFormat.format("{0}", processId);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    KeyDataResponse response = new KeyDataResponse(output, processId, secret);

    assertEquals('K', response.getIdentifier());
    assertEquals("Key Data Information", response.getMessageName());
    String payloadString = response.getPayloadString();
    assertTrue(payloadString.contains("Process ID: " + formattedProcessId));
    assertTrue(payloadString.contains("<REDACTED>"));
    assertFalse(payloadString.contains(Arrays.toString(secret)));

    Logger wireOutputLogger = Logger.getLogger(WireOutput.class.getName());
    Level previousLevel = wireOutputLogger.getLevel();
    boolean previousUseParentHandlers = wireOutputLogger.getUseParentHandlers();
    List<String> loggedMessages = new ArrayList<>();
    Handler testHandler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            loggedMessages.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    try {
      wireOutputLogger.setUseParentHandlers(false);
      wireOutputLogger.addHandler(testHandler);
      wireOutputLogger.setLevel(Level.FINEST);
      response.send(false);
    } finally {
      wireOutputLogger.removeHandler(testHandler);
      wireOutputLogger.setLevel(previousLevel);
      wireOutputLogger.setUseParentHandlers(previousUseParentHandlers);
    }

    String targetMessage = null;
    for (String message : loggedMessages) {
      assertFalse(message.contains(Arrays.toString(secret)));
      if (message.contains("Process ID: " + formattedProcessId)) {
        targetMessage = message;
      }
    }
    assertNotNull(targetMessage);
    assertTrue(targetMessage.contains("<REDACTED>"));

    ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
    DataOutputStream expectedOutput = new DataOutputStream(expectedBytes);
    expectedOutput.writeByte('K');
    expectedOutput.writeInt(8 + secret.length);
    expectedOutput.writeInt(processId);
    expectedOutput.write(secret);
    assertArrayEquals(expectedBytes.toByteArray(), bytes.toByteArray());
  }
}
