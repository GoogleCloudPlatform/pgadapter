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

package com.google.cloud.spanner.pgadapter.python.django;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DjangoTestSetup extends DjangoMockServerTest {

  static boolean isPythonAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] pythonCommand = new String[] {"python3", "--version"};
    builder.command(pythonCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static String DJANGO_PATH = "./src/test/python/django";

  private static String execute(int port, String host, List<String> options, String testFileName)
      throws IOException, InterruptedException {
    List<String> runCommand =
        new ArrayList<>(Arrays.asList("python3", testFileName, host, Integer.toString(port)));
    runCommand.addAll(options);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File(DJANGO_PATH));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());

    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(output.toString(), 0, result);

    return output.toString();
  }

  public String executeBasicTests(int port, String host, List<String> options)
      throws IOException, InterruptedException {
    return execute(port, host, options, "basic_test.py");
  }

  public String executeTransactionTests(int port, String host, List<String> options)
      throws IOException, InterruptedException {
    return execute(port, host, options, "transaction_test.py");
  }

  public String executeConditionalTests(int port, String host, List<String> options)
      throws IOException, InterruptedException {
    return execute(port, host, options, "conditional_expressions_test.py");
  }

  public String executeAggregationTests(int port, String host, List<String> options)
      throws IOException, InterruptedException {
    return execute(port, host, options, "aggregation_tests.py");

  }
}
