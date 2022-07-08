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

/** Golang Test interface. */
public interface GolangTest {

  /** Compiles a Go test file into a C shared library, so it can be called from Java using JNA. */
  static <T extends Library> T compile(String goTestFileName, Class<T> testClass)
      throws IOException, InterruptedException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testFilePath = String.format("%s/src/test/golang/%s", currentPath, goTestFileName);
    File testFile = new File(testFilePath);
    String testFileNameWithoutExtension = Files.getNameWithoutExtension(testFile.getName());
    File directory = testFile.getParentFile();
    // Compile the Go code to ensure that we always have the most recent test code.
    ProcessBuilder builder = new ProcessBuilder();
    String[] compileCommand =
        String.format(
                "go build -o %s_test.so -buildmode=c-shared %s",
                testFileNameWithoutExtension, testFile.getName())
            .split(" ");
    builder.command(compileCommand);
    builder.directory(directory);
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    // We explicitly use the full path to force JNA to look in a specific directory, instead of in
    // standard library directories.
    return Native.load(
        String.format(
            "%s/src/test/golang/%s/%s_test.so",
            currentPath, directory.getName(), testFileNameWithoutExtension),
        testClass);
  }
}
