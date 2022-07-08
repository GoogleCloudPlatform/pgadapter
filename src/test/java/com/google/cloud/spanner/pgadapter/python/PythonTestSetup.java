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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PythonTestSetup extends AbstractMockServerTest {
  static String executeWithoutParameters(int port, String sql, String statementType)
      throws IOException, InterruptedException {
    String[] runCommand =
        new String[] {
          "python3", "StatementsWithoutParameters.py", Integer.toString(port), statementType, sql
        };
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
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

  static String executeWithParameters(
      int port, String sql, String statementType, ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithParameters.py");
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
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

  static String executeWithNamedParameters(
      int port, String sql, String statementType, ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithNamedParameters.py");
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
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

  static String executeInBatch(
      int port, String sql, String statementType, ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("Batching.py");
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
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

}
