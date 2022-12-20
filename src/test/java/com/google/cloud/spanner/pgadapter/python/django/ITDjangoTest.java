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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class ITDjangoTest extends DjangoTestSetup {

  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() throws Exception {
    if (!isPythonAvailable()
        && !System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
            .equalsIgnoreCase("true")) {
      throw new IllegalStateException(
          "python has not been set up on this system and allowSkipUnsupportedEcosystemTest has not been set");
    }

    testEnv.setUp();
    database = testEnv.createDatabase(Collections.emptyList());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    String actualOutput = executeIntegrationTests();
    String expectedOutput = "Django Test Completed Successfully\n";
    assertTrue(actualOutput.contains(expectedOutput));
  }
}
