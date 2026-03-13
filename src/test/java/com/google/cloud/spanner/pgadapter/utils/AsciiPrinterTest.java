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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AsciiPrinterTest {

  @Test
  public void testWriteEmptyString() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(buffer);

    AsciiPrinter.writeAsciiToPG(dataOutputStream, "");
    dataOutputStream.flush();

    byte[] result = buffer.toByteArray();
    // Int 0 is 4 bytes of 0s
    assertArrayEquals(new byte[] {0, 0, 0, 0}, result);
  }

  @Test
  public void testWriteNormalString() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(buffer);

    String value = "123.45e-10";
    AsciiPrinter.writeAsciiToPG(dataOutputStream, value);
    dataOutputStream.flush();

    byte[] result = buffer.toByteArray();
    ByteBuffer byteBuffer = ByteBuffer.wrap(result);

    assertEquals(value.length(), byteBuffer.getInt());
    byte[] strBytes = new byte[value.length()];
    byteBuffer.get(strBytes);
    assertEquals(value, new String(strBytes, StandardCharsets.US_ASCII));
  }

  @Test
  public void testWriteLongStringreallocatesBuffer() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(buffer);

    // Create a string longer than the initial 64 byte buffer
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append((i % 10)); // "01234567890123..."
    }
    String value = sb.toString();

    AsciiPrinter.writeAsciiToPG(dataOutputStream, value);
    dataOutputStream.flush();

    byte[] result = buffer.toByteArray();
    ByteBuffer byteBuffer = ByteBuffer.wrap(result);

    assertEquals(value.length(), byteBuffer.getInt());
    byte[] strBytes = new byte[value.length()];
    byteBuffer.get(strBytes);
    assertEquals(value, new String(strBytes, StandardCharsets.US_ASCII));
  }
}
