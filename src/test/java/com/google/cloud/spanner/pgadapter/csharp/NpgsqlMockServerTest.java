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

package com.google.cloud.spanner.pgadapter.csharp;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NpgsqlMockServerTest extends AbstractMockServerTest {

  @Test
  public void testPing() throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    String[] compileCommand = "dotnet run".split(" ");
    builder.command(compileCommand);
    builder.directory(new File("./src/test/csharp/pgadapter_npgsql_tests/npgsql_tests"));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    String output = scanner.nextLine();
    int res = process.waitFor();
    assertEquals(0, res);
    assertEquals("Here is C#", output);
  }
}
