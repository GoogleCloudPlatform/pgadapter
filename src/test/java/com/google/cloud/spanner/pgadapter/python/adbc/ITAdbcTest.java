// Copyright 2026 Google LLC
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

package com.google.cloud.spanner.pgadapter.python.adbc;

import static com.google.cloud.spanner.pgadapter.PgAdapterTestEnv.getOnlyAllTypesDdl;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.python.PythonTestUtil;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class ITAdbcTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled()
        ? new Object[] {"localhost", "/tmp"}
        : new Object[] {"localhost"};
  }

  @BeforeClass
  public static void setup() throws Exception {
    Logger.getLogger("com.google.cloud.spanner.pgadapter").setLevel(Level.FINEST);
    for (Handler handler : Logger.getLogger("").getHandlers()) {
      handler.setLevel(Level.FINEST);
    }

    // We reuse the virtual environment from AdbcMockServerTest
    PythonTestUtil.createVirtualEnv(AdbcMockServerTest.DIRECTORY_NAME);
    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private static Iterable<String> getDdlStatements() {
    return getOnlyAllTypesDdl();
  }

  private String createConnectionString() {
    return String.format(
        "host=%s port=%d dbname=d sslmode=disable", host, testEnv.getServer().getLocalPort());
  }

  private String execute(String method) throws Exception {
    return AdbcMockServerTest.execute(method, createConnectionString());
  }

  @Test
  public void testSelect1() throws Exception {
    String actualOutput = execute("select1");
    String expectedOutput = "(1,)\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
