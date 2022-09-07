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

  static String executeWithoutParameters(
      String pgVersion, String host, int port, String sql, String statementType)
      throws IOException, InterruptedException {
    String[] runCommand =
        new String[] {
          "python3",
          "StatementsWithoutParameters.py",
          pgVersion,
          host,
          Integer.toString(port),
          statementType,
          sql
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
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithParameters.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
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
    Scanner errorScanner = new Scanner(process.getErrorStream());
    StringBuilder errors = new StringBuilder();
    while (errorScanner.hasNextLine()) {
      errors.append(errorScanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(errors.toString(), 0, result);

    return output.toString();
  }

  static String executeWithNamedParameters(
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithNamedParameters.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);

    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
    Process process = builder.start();

    Scanner errorScanner = new Scanner(process.getErrorStream());
    Scanner scanner = new Scanner(process.getInputStream());
    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    StringBuilder error = new StringBuilder();
    while (errorScanner.hasNextLine()) {
      error.append(errorScanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(error.toString(), 0, result);

    return output.toString();
  }

  static String executeInBatch(
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("Batching.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
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

  static String executeTransactions(
      String pgVersion, String host, int port, List<String> statements)
      throws IOException, InterruptedException {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithTransactions.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.addAll(statements);

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

  static String executeCopy(int port, String sql, String file, String copyType)
      throws IOException, InterruptedException {
    String[] runCommand =
        new String[] {
          "python3", "StatementsWithCopy.py", Integer.toString(port), sql, file, copyType
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
}
