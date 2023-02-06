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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Scanner;

public class PythonTestUtil {

  public static void createVirtualEnv(String directoryName, String psycopgVersion)
      throws Exception {
    File directory = new File(directoryName);
    run(new String[] {"python3", "-m", "venv", "venv"}, directoryName);
    run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/python",
          "-m",
          "pip",
          "install",
          "--upgrade",
          "pip"
        },
        directoryName);
    run(
        new String[] {
          directory.getAbsolutePath() + "/venv/bin/pip", "install", "-r", "requirements.txt"
        },
        directoryName);
    // Make sure that psycopg is available in the venv.
    //    PythonTestUtil.run(new String[]{
    //        directory.getAbsolutePath() + "/venv/bin/pip",
    //        "install", psycopgVersion
    //    }, directoryName);
  }

  public static String run(String[] command, String directoryName) throws Exception {
    File directory = new File(directoryName);
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(command);
    builder.directory(directory);
    builder.environment().put("VIRTUAL_ENV", directoryName + "/venv");
    builder
        .environment()
        .put(
            "C_INCLUDE_PATH",
            "/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.8/Headers");
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    Scanner errorScanner = new Scanner(process.getErrorStream());

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
}
