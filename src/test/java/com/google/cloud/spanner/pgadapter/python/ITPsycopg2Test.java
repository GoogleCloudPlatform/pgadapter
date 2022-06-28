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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import java.io.IOException;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class ITPsycopg2Test extends PythonTestSetup implements IntegrationTest {

  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  private static Iterable<String> getDdlStatements() {
    return Collections.singletonList(
        "create table all_types ("
            + "col_bigint bigint not null primary key, "
            + "col_bool bool, "
            + "col_bytea bytea, "
            + "col_float8 float8, "
            + "col_int int, "
            + "col_numeric numeric, "
            + "col_timestamptz timestamptz, "
            + "col_date date, "
            + "col_varchar varchar(100))");
  }

  @BeforeClass
  public static void setup() throws Exception {
    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testBasicInsert() throws IOException, InterruptedException {
    String sql =
        "Insert into all_types("
            + "col_bigint, "
            + "col_bool , "
            + "col_bytea , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar )"
            + " values("
            + "2, "
            + "TRUE, "
            + "'some string'::bytea, "
            + "9.7, "
            + "10, "
            + "300.321, "
            + "'2018-03-11 02:00:00'::timestamptz, "
            + "'2022-10-02', "
            + "'some string')";

    String actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
