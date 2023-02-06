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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/** NodeJS Test interface. */
public interface NodeJSTest {

  static void installDependencies(String directory) throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/nodejs/%s", currentPath, directory);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command("npm", "install");
    builder.directory(new File(testFilePath));

    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);
  }

  static String runTest(String directory, String testName, String host, int port, String database)
      throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/nodejs/%s", currentPath, directory);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(
        "npm", "--silent", "start", testName, host, String.format("%d", port), database);
    builder.directory(new File(testFilePath));

    Process process = builder.start();
    InputStream inputStream = process.getInputStream();
    InputStream errorStream = process.getErrorStream();
    boolean finished = process.waitFor(120L, TimeUnit.SECONDS);

    String output = readAll(inputStream);
    String errors = readAll(errorStream);
    assertEquals("", errors);
    assertTrue(finished);
    assertEquals(0, process.exitValue());

    return output;
  }

  static String readAll(InputStream inputStream) {
    StringBuilder result = new StringBuilder();
    try (Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        // Skip lines that are generated by npm / node.
        if (!line.startsWith(">")) {
          result.append(line).append("\n");
        }
      }
    }
    return result.toString();
  }
}
