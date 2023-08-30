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

package com.google.cloud.spanner.pgadapter.python.sqlalchemy;

import static com.google.cloud.spanner.pgadapter.python.sqlalchemy.SqlAlchemyBasicsTest.execute;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.SlowTest;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({IntegrationTest.class, SlowTest.class})
@RunWith(JUnit4.class)
public class ITSQLAlchemySampleTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static final String SAMPLE_DIR = "./samples/python/sqlalchemy-sample";

  @BeforeClass
  public static void setup() throws Exception {
    PythonTestUtil.createVirtualEnv(SAMPLE_DIR);
    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testSQLAlchemySample() throws Exception {
    String output =
        execute(
            SAMPLE_DIR,
            "run_sample.py",
            "localhost",
            testEnv.getServer().getLocalPort(),
            testEnv.getDatabaseId());
    assertNotNull(output);
    assertTrue(
        output,
        output.contains("No album found using a stale read.")
            || output.contains(
                "Album was found using a stale read, even though it has already been deleted."));
  }
}
