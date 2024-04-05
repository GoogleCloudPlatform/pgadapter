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

package com.google.example;

import static com.google.example.CreateTables.createTables;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class SamplesTest {
  private static final String DATABASE_ID = "test-database";

  private static GenericContainer<?> emulator;

  @BeforeClass
  public static void setup() {
    emulator =
        new GenericContainer<>(
                DockerImageName.parse("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator:latest"))
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    emulator.start();
  }

  @Test
  public void testSamples() throws SQLException {
    assertEquals(
        "Created Singers & Albums tables in database: [" + DATABASE_ID + "]\n",
        runSample(
            () -> createTables(emulator.getHost(), emulator.getMappedPort(5432), DATABASE_ID)));
  }

  interface Sample {
    void run() throws SQLException;
  }

  String runSample(Sample sample) throws SQLException {
    PrintStream stdOut = System.out;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    System.setOut(out);
    try {
      sample.run();
    } finally {
      System.setOut(stdOut);
    }
    return bout.toString();
  }
}
