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

import com.sun.jna.Library;
import com.sun.jna.Native;
import java.io.File;
import java.io.IOException;

public interface PgxTest extends Library {

  String TestHelloWorld(GoString connString);

  String TestSelect1(GoString connString);

  String TestQueryWithParameter(GoString connString);

  String TestQueryAllDataTypes(GoString connString, boolean binary);

  String TestInsertAllDataTypes(GoString connString, boolean floatAndNumericSupported);

  String TestInsertNullsAllDataTypes(GoString connString);

  String TestWrongDialect(GoString connString);

  static PgxTest compile() throws IOException, InterruptedException {
    // Compile the Go code to ensure that we always have the most recent test code.
    ProcessBuilder builder = new ProcessBuilder();
    String[] compileCommand = "go build -o pgx_test.so -buildmode=c-shared pgx.go".split(" ");
    builder.command(compileCommand);
    builder.directory(new File("./src/test/golang/pgadapter_pgx_tests"));
    Process process = builder.start();
    int res = process.waitFor();
    assertEquals(0, res);

    // We explicitly use the full path to force JNA to look in a specific directory, instead of in
    // standard library directories.
    String currentPath = new java.io.File(".").getCanonicalPath();
    return Native.load(
        String.format("%s/src/test/golang/pgadapter_pgx_tests/pgx_test.so", currentPath),
        PgxTest.class);
  }
}
