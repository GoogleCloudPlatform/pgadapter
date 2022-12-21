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

package com.google.cloud.spanner.pgadapter.wireoutput;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StartupMessageResponseTest {

  @Test
  public void testMessage() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    StartUpMessageResponse response =
        new StartUpMessageResponse(
            output,
            "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8));
    assertEquals('S', response.getIdentifier());
    assertEquals("Start-Up", response.getMessageName());

    response.sendPayload();

    assertArrayEquals("key\0value\0".getBytes(StandardCharsets.UTF_8), bytes.toByteArray());
  }
}
