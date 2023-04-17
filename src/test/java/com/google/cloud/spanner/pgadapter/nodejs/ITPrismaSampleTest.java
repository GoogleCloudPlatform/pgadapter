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

package com.google.cloud.spanner.pgadapter.nodejs;

import static com.google.cloud.spanner.pgadapter.nodejs.PrismaSampleTest.runTest;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public class ITPrismaSampleTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  static File getTestDirectory() throws IOException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testDirectoryPath = String.format("%s/samples/nodejs/prisma/src", currentPath);
    return new File(testDirectoryPath);
  }

  static String runTest(String testName) throws IOException, InterruptedException {
    return NodeJSTest.runTest(
        getTestDirectory(),
        "test",
        testName,
        "localhost",
        testEnv.getPGAdapterPort(),
        database.getId().getDatabase());
  }

  @BeforeClass
  public static void setup() throws Exception {
    NodeJSTest.installDependencies(getTestDirectory());

    testEnv.setUp();
    database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testCreateRandomSingersAndAlbums() throws Exception {
    String output = runTest("testCreateRandomSingersAndAlbums");

    assertTrue(output, output.startsWith("Creating random singers and albums\n"));
    assertTrue(output, output.matches("(?s).*Created \\d+ singers and \\d+ albums.*"));
  }

  @Test
  public void testPrintSingersAndAlbums() throws Exception {
    String output = runTest("testPrintSingersAndAlbums");

    assertTrue(output, output.startsWith("Creating random singers and albums\n"));
  }
}
