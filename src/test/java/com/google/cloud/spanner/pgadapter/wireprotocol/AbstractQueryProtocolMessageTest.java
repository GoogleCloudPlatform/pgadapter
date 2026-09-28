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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractQueryProtocolMessageTest {

  @Test
  public void testReceivedEventDescription() {
    assertEquals("Received message: 'P'", receivedEventDescription(ParseMessage.class));
    assertEquals("Received message: 'B'", receivedEventDescription(BindMessage.class));
    assertEquals("Received message: 'D'", receivedEventDescription(DescribeMessage.class));
    assertEquals("Received message: 'E'", receivedEventDescription(ExecuteMessage.class));
  }

  @Test
  public void testReceivedEventDescriptionIsSharedBetweenMessages() {
    // The description is requested for every message that is received, so it must be a constant per
    // message type instead of a string that is created for each message.
    assertSame(
        receivedEventDescription(ParseMessage.class), receivedEventDescription(ParseMessage.class));
    assertSame(
        receivedEventDescription(BindMessage.class), receivedEventDescription(BindMessage.class));
    assertSame(
        receivedEventDescription(DescribeMessage.class),
        receivedEventDescription(DescribeMessage.class));
    assertSame(
        receivedEventDescription(ExecuteMessage.class),
        receivedEventDescription(ExecuteMessage.class));
  }

  /**
   * Returns the event description for the given message type. The description does not depend on
   * any instance state, so the message is mocked to avoid having to set up a connection for each
   * message type.
   */
  private static String receivedEventDescription(
      Class<? extends AbstractQueryProtocolMessage> messageType) {
    return mock(messageType, CALLS_REAL_METHODS).receivedEventDescription();
  }
}
