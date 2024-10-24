// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SampleApplicationTest {

  @Test
  public void testRunApplication() {
    ByteArrayOutputStream outArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outArrayStream);
    PrintStream originalOut = System.out;
    System.setOut(out);
    try {
      SampleApplication.main(new String[] {});
      String output = outArrayStream.toString();
      assertTrue(
          output, output.contains("Found 51 concerts using a query with directed read options"));
    } finally {
      System.setOut(originalOut);
    }
  }
}
