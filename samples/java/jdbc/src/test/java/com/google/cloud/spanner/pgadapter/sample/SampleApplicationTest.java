// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SampleApplicationTest {

  @Test
  public void testRunApplication() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    ImmutableList<String> command = ImmutableList.of("mvn", "exec:java");
    builder.command(command);
    Process process = builder.start();

    String errors;
    String output;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    int res = process.waitFor();
    assertEquals(errors, 0, res);
    assertTrue(output, output.contains("Greeting: Hello World!"));
  }
}
