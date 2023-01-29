// Copyright 2023 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.wireoutput.NoticeResponse.NoticeSeverity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NoticeResponseTest {

  @Test
  public void testBasics() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    NoticeResponse response =
        new NoticeResponse(output, SQLState.Success, NoticeSeverity.NOTICE, "test notice", null);

    assertEquals('N', response.getIdentifier());
    assertEquals("Notice", response.getMessageName());
  }

  @Test
  public void testSendPayload() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    NoticeResponse response =
        new NoticeResponse(output, SQLState.Success, NoticeSeverity.NOTICE, "test notice", null);
    response.sendPayload();

    assertEquals("SNOTICE\0C00000\0Mtest notice\0\0", out.toString());
    assertEquals(
        "Length: 33, Severity: NOTICE, Notice Message: test notice, Hint: ",
        response.getPayloadString());
  }

  @Test
  public void testSendPayloadWithHint() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(out);
    NoticeResponse response =
        new NoticeResponse(
            output, SQLState.Success, NoticeSeverity.NOTICE, "test notice", "some hint");
    response.sendPayload();

    assertEquals("SNOTICE\0C00000\0Mtest notice\0Hsome hint\0\0", out.toString());
    assertEquals(
        "Length: 44, Severity: NOTICE, Notice Message: test notice, Hint: some hint",
        response.getPayloadString());
  }
}
