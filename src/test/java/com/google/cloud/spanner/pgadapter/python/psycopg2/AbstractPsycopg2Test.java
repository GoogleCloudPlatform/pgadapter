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

package com.google.cloud.spanner.pgadapter.python.psycopg2;


import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;

abstract class AbstractPsycopg2Test extends AbstractMockServerTest {

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

  private static final String PSYCOPG2_PATH = "./src/test/python/psycopg2";

  @BeforeClass
  public static void createVirtualEnv() throws Exception {
    PythonTestUtil.createVirtualEnv(PSYCOPG2_PATH);
  }

  static String executeWithoutParameters(
      String pgVersion, String host, int port, String sql, String statementType) throws Exception {
    return PythonTestUtil.run(
        new String[] {
          "python3",
          "StatementsWithoutParameters.py",
          pgVersion,
          host,
          Integer.toString(port),
          statementType,
          sql
        },
        PSYCOPG2_PATH);
  }

  static String executeWithParameters(
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws Exception {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithParameters.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);
    return PythonTestUtil.run(runCommand.toArray(new String[0]), PSYCOPG2_PATH);
  }

  static String executeWithNamedParameters(
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws Exception {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithNamedParameters.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);
    return PythonTestUtil.run(runCommand.toArray(new String[0]), PSYCOPG2_PATH);
  }

  static String executeInBatch(
      String pgVersion,
      String host,
      int port,
      String sql,
      String statementType,
      ArrayList<String> parameters)
      throws Exception {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("Batching.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.add(statementType);
    runCommand.add(sql);
    runCommand.addAll(parameters);
    return PythonTestUtil.run(runCommand.toArray(new String[0]), PSYCOPG2_PATH);
  }

  static String executeTransactions(
      String pgVersion, String host, int port, List<String> statements) throws Exception {
    List<String> runCommand = new ArrayList<>();
    runCommand.add("python3");
    runCommand.add("StatementsWithTransactions.py");
    runCommand.add(pgVersion);
    runCommand.add(host);
    runCommand.add(Integer.toString(port));
    runCommand.addAll(statements);
    return PythonTestUtil.run(runCommand.toArray(new String[0]), PSYCOPG2_PATH);
  }

  static String executeCopy(int port, String sql, String file, String copyType) throws Exception {
    return PythonTestUtil.run(
        new String[] {
          "python3", "StatementsWithCopy.py", Integer.toString(port), sql, file, copyType
        },
        PSYCOPG2_PATH);
  }
}
