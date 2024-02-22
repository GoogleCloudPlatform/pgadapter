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

package com.google.cloud.spanner.pgadapter.latency;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RunBenchmarkTest {

  @Test
  public void testRunBenchmark() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "mvn",
        "clean",
        "compile",
        "exec:java",
        "-Dexec.args=--clients=16 --operations=1000 -skip_pg -skip_jdbc -skip_spanner");
    builder.environment().put("GOOGLE_CLOUD_PROJECT", "test-project");
    builder.environment().put("SPANNER_INSTANCE", "test-instance");
    builder.environment().put("SPANNER_DATABASE", "test-database");

    Process process = builder.start();
    InputStream inputStream = process.getInputStream();
    InputStream errorStream = process.getErrorStream();
    int res = process.waitFor();
    String output = readAll(inputStream);
    String errors = readAll(errorStream);
    assertEquals(output + "\n" + errors, 0, res);
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        result.append(scanner.nextLine()).append("\n");
      }
    }
    return result.toString();
  }
}
