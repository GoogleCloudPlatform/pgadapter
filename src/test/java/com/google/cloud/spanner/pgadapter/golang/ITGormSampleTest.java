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

import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import com.google.common.collect.ImmutableList;
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
public class ITGormSampleTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static GormSampleTest gormSampleTest;

  @BeforeClass
  public static void setup() throws Exception {
    try {
      gormSampleTest =
          GolangTest.compile("../../../samples/golang/gorm/sample.go", GormSampleTest.class);
    } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
      // This probably means that there is a version mismatch for GLIBC (or no GLIBC at all
      // installed).
      assumeFalse(
          "Skipping ecosystem test because of missing dependency",
          System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
              .equalsIgnoreCase("true"));
      throw unsatisfiedLinkError;
    }

    testEnv.setUp();
    Database database = testEnv.createDatabase(ImmutableList.of());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private GoString createConnString() {
    return new GoString(String.format("host=/tmp port=%d", testEnv.getServer().getLocalPort()));
  }

  @Test
  public void testGormSample() throws IOException {
    assertNull(
        gormSampleTest.TestRunSample(
            createConnString(),
            new GoString(new java.io.File(".").getCanonicalPath() + "/samples/golang/gorm")));
  }
}
