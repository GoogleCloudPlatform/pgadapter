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

import static org.junit.Assert.assertArrayEquals;

import com.google.cloud.spanner.pgadapter.ProxyServer.DataFormat;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CopyInResponseTest {

  @Test
  public void testSendPayloadText() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    short numColumns = 3;

    CopyInResponse response =
        new CopyInResponse(output, numColumns, (byte) DataFormat.POSTGRESQL_TEXT.getCode());
    response.sendPayload();

    assertArrayEquals(
        new byte[] {
          0, // format
          0, 3, // numColumns
          0, 0, // format column 1
          0, 0, // format column 2
          0, 0, // format column 3
        },
        bytes.toByteArray());
  }

  @Test
  public void testSendPayloadBinary() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    short numColumns = 3;

    CopyInResponse response =
        new CopyInResponse(output, numColumns, (byte) DataFormat.POSTGRESQL_BINARY.getCode());
    response.sendPayload();

    assertArrayEquals(
        new byte[] {
          1, // format
          0, 3, // numColumns
          0, 1, // format column 1
          0, 1, // format column 2
          0, 1, // format column 3
        },
        bytes.toByteArray());
  }
}
