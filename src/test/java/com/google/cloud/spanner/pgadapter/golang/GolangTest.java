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

package com.google.cloud.spanner.pgadapter.golang;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import com.sun.jna.Library;
import com.sun.jna.Native;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/** Golang Test interface. */
public interface GolangTest {

  static File getTestDirectory(String goTestFileName) throws IOException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/golang/%s", currentPath, goTestFileName);
    File testFile = new File(testFilePath);
    String testFileNameWithoutExtension = Files.getNameWithoutExtension(testFile.getName());
    return testFile.getParentFile();
  }

  /** Compiles a Go test file into a C shared library, so it can be called from Java using JNA. */
  static <T extends Library> T compile(String goTestFileName, Class<T> testClass)
      throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/golang/%s", currentPath, goTestFileName);
    File testFile = new File(testFilePath);
    String testFileNameWithoutExtension = Files.getNameWithoutExtension(testFile.getName());
    File directory = testFile.getParentFile();
    // Get all dependencies for this module.
    run(new String[] {"go", "mod", "tidy"}, directory);
    // Compile the Go code to ensure that we always have the most recent test code.
    run(
        String.format(
                "go build -o %s_test.so -buildmode=c-shared %s",
                testFileNameWithoutExtension, testFile.getName())
            .split(" "),
        directory);

    // We explicitly use the full path to force JNA to look in a specific directory, instead of in
    // standard library directories.
    return Native.load(
        String.format("%s/%s_test.so", directory.getAbsolutePath(), testFileNameWithoutExtension),
        testClass);
  }

  static void run(String[] command, File directory) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(command);
    builder.directory(directory);
    Process process = builder.start();
    Scanner errorScanner = new Scanner(process.getErrorStream());
    StringBuilder error = new StringBuilder();
    while (errorScanner.hasNextLine()) {
      error.append(errorScanner.nextLine()).append("\n");
    }
    int res = process.waitFor();
    assertEquals(error.toString(), 0, res);
  }
}
