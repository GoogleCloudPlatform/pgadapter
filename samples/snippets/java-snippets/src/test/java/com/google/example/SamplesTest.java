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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
  private static final String DATABASE_ID = "example-db";

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
  public void testSamples() throws SQLException, IOException {
    assertEquals(
        "Created Singers & Albums tables in database: [" + DATABASE_ID + "]\n",
        runSample(CreateTables::createTables));
    assertEquals(
        "Greeting from Cloud Spanner PostgreSQL: Hello world!\n",
        runSample(CreateConnection::createConnection));
    assertEquals("4 records inserted.\n", runSample(WriteDataWithDml::writeDataWithDml));
    assertEquals("3 records inserted.\n", runSample(WriteDataWithDmlBatch::writeDataWithDmlBatch));
    assertEquals(
        "Copied 5 singers\n" + "Copied 5 albums\n",
        runSample(WriteDataWithCopy::writeDataWithCopy));
    assertEquals(
        "1 2 Go, Go, Go\n"
            + "2 2 Forever Hold Your Peace\n"
            + "1 1 Total Junk\n"
            + "2 1 Green\n"
            + "2 3 Terrified\n",
        runSample(QueryData::queryData));
    assertEquals("12 Melissa Garcia\n", runSample(QueryDataWithParameter::queryDataWithParameter));
    assertEquals("", runSample(StatementTimeout::queryWithTimeout));
    assertEquals("Added marketing_budget column\n", runSample(AddColumn::addColumn));
    assertEquals("Added venues and concerts tables\n", runSample(DdlBatch::ddlBatch));
    assertEquals("Updated 2 albums\n", runSample(UpdateDataWithCopy::updateDataWithCopy));
    assertEquals(
        "1 1 100000\n" + "1 2 null\n" + "2 1 null\n" + "2 2 500000\n" + "2 3 null\n",
        runSample(QueryDataWithNewColumn::queryDataWithNewColumn));
    assertEquals(
        "Transferred marketing budget from Album 2 to Album 1\n",
        runSample(UpdateDataWithTransaction::writeWithTransactionUsingDml));
    assertEquals("Reduced marketing budget\n", runSample(Tags::tags));
    assertEquals(
        "1 1 Total Junk\n"
            + "1 2 Go, Go, Go\n"
            + "2 1 Green\n"
            + "2 2 Forever Hold Your Peace\n"
            + "2 3 Terrified\n"
            + "2 2 Forever Hold Your Peace\n"
            + "1 2 Go, Go, Go\n"
            + "2 1 Green\n"
            + "2 3 Terrified\n"
            + "1 1 Total Junk\n",
        runSample(ReadOnlyTransaction::readOnlyTransaction));
    assertEquals(
        "2 Catalina Smith\n"
            + "4 Lea Martin\n"
            + "12 Melissa Garcia\n"
            + "14 Jacqueline Long\n"
            + "16 Sarah Wilson\n"
            + "18 Maya Patel\n"
            + "1 Marc Richards\n"
            + "3 Alice Trentor\n"
            + "5 David Lomond\n"
            + "13 Russel Morales\n"
            + "15 Dylan Shaw\n"
            + "17 Ethan Miller\n",
        runSample(DataBoost::dataBoost));
    assertEquals("Updated at least 3 albums\n", runSample(PartitionedDml::partitionedDml));
  }

  interface Sample {
    void run(String host, int port, String database) throws SQLException, IOException;
  }

  String runSample(Sample sample) throws SQLException, IOException {
    PrintStream stdOut = System.out;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    System.setOut(out);
    try {
      sample.run(emulator.getHost(), emulator.getMappedPort(5432), DATABASE_ID);
    } finally {
      System.setOut(stdOut);
    }
    return bout.toString();
  }
}
