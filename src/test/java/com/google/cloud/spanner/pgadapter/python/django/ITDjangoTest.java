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

package com.google.cloud.spanner.pgadapter.python.django;

import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class ITDjangoTest extends DjangoTestSetup {

  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static final String DJANGO_SETTING_FILE = "samples/python/django/setting.py";
  private static String originalDjangoSetting = "";
  private static final String DEFAULT_PORT = "5432";

  @BeforeClass
  public static void setup() throws Exception {
    if (!isPythonAvailable()
        && !System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
            .equalsIgnoreCase("true")) {
      throw new IllegalStateException(
          "python has not been set up on this system and allowSkipUnsupportedEcosystemTest has not been set");
    }

    PythonTestUtil.createVirtualEnv(DJANGO_SAMPLES_PATH);
    testEnv.setUp();
    Database database = testEnv.createDatabase(Collections.emptyList());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());

    // Update Django setting.py file with the PG Adapter Port.properties
    StringBuilder original = new StringBuilder();
    try (Scanner scanner = new Scanner(new FileReader(DJANGO_SETTING_FILE))) {
      while (scanner.hasNextLine()) {
        original.append(scanner.nextLine()).append("\n");
      }
    }
    originalDjangoSetting = original.toString();
    String updatedDjangoSetting =
        original.toString().replace(DEFAULT_PORT, Integer.toString(testEnv.getPGAdapterPort()));
    try (FileWriter writer = new FileWriter(DJANGO_SETTING_FILE)) {
      writer.write(updatedDjangoSetting);
      writer.flush();
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    try (FileWriter writer = new FileWriter(DJANGO_SETTING_FILE)) {
      writer.write(originalDjangoSetting);
    }
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testRunSample() throws Exception {
    String actualOutput = executeIntegrationTests();
    String expectedOutput = "Django Sample Completed Successfully\n";
    assertTrue(actualOutput, actualOutput.contains(expectedOutput));
  }
}
