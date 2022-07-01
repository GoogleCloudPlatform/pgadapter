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
import java.util.ArrayList;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITPsycopg2Test extends PythonTestSetup {

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
  public void test1Insert() throws IOException, InterruptedException {
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

  @Test
  public void test2Select() throws IOException, InterruptedException {
    String sql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types where col_bigint = 2";
    String actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "query");
    String expectedOutput =
        "(2, True, 9.7, 10, Decimal('300.321'), datetime.datetime(2018, 3, 11, 10, 0, tzinfo=datetime.timezone.utc), datetime.date(2022, 10, 2), 'some string')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test3Update() throws IOException, InterruptedException {
    String sql =
        "UPDATE all_types SET "
            + "col_bool = 'off',"
            + "col_float8 = 3.14, "
            + "col_int = 20, "
            + "col_numeric = 99.99, "
            + "col_timestamptz = '2019-05-12 14:30:00'::timestamptz, "
            + "col_date = '1989-09-02', "
            + "col_varchar = 'new string' "
            + "where col_bigint = 2";

    String actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types where col_bigint = 2";

    actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "query");
    expectedOutput =
        "(2, False, 3.14, 20, Decimal('99.99'), datetime.datetime(2019, 5, 12, 21, 30, tzinfo=datetime.timezone.utc), datetime.date(1989, 9, 2), 'new string')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test4Delete() throws IOException, InterruptedException {
    String sql = "DELETE FROM all_types where col_bigint = 2";

    String actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql = "Select COUNT(*) from all_types where col_bigint = 2";

    actualOutput = executeWithoutParameters(testEnv.getPGAdapterPort(), sql, "query");
    expectedOutput = "(0,)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test1InsertParametrized() throws IOException, InterruptedException {
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
            + "%(bigint)s, "
            + "%(bool)s, "
            + "%(bytea)s, "
            + "%(float)s, "
            + "%(int)s, "
            + "%(numeric)s, "
            + "%(tstz)s, "
            + "%(date)s, "
            + "%(string)s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("bool");
    parameters.add("bool");
    parameters.add("True");
    parameters.add("float");
    parameters.add("float");
    parameters.add("89.23");
    parameters.add("numeric");
    parameters.add("float");
    parameters.add("33.546");
    parameters.add("tstz");
    parameters.add("datetime.datetime");
    parameters.add("2019, 2, 3, 6, 30, 15, 0, pytz.UTC");
    parameters.add("string");
    parameters.add("str");
    parameters.add("'hello world'");
    parameters.add("date");
    parameters.add("datetime.date");
    parameters.add("2022, 7, 1");
    parameters.add("bigint");
    parameters.add("int");
    parameters.add("3");
    parameters.add("bytea");
    parameters.add("memoryview");
    parameters.add("b'alpha beta'");
    parameters.add("int");
    parameters.add("int");
    parameters.add("34");

    String actualOutput =
        executeWithNamedParameters(testEnv.getPGAdapterPort(), sql, "data_type_update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test2SelectParametrized() throws IOException, InterruptedException {
    String sql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types where col_varchar = %s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("hello world");

    String actualOutput =
        executeWithParameters(testEnv.getPGAdapterPort(), sql, "query", parameters);
    String expectedOutput =
        "(3, True, 89.23, 34, Decimal('33.546'), datetime.datetime(2019, 2, 3, 6, 30, 15, tzinfo=datetime.timezone.utc), datetime.date(2022, 7, 1), 'hello world')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test3UpdateParametrized() throws IOException, InterruptedException {
    String sql =
        "UPDATE all_types SET "
            + "col_bool = %(bool)s,"
            + "col_float8 = %(float8)s, "
            + "col_int = %(int)s, "
            + "col_numeric = %(numeric)s, "
            + "col_timestamptz = %(tstz)s, "
            + "col_date = %(date)s, "
            + "col_varchar = %(string)s "
            + "where col_bigint = %(bigint)s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("bool");
    parameters.add("bool");
    parameters.add("False");
    parameters.add("float8");
    parameters.add("float");
    parameters.add("98.6");
    parameters.add("numeric");
    parameters.add("float");
    parameters.add("77.46");
    parameters.add("tstz");
    parameters.add("datetime.datetime");
    parameters.add("2022, 10, 2, 14, 34, 26, 0, pytz.UTC");
    parameters.add("string");
    parameters.add("str");
    parameters.add("'hello psycopg2'");
    parameters.add("date");
    parameters.add("datetime.date");
    parameters.add("2021, 12, 23");
    parameters.add("bigint");
    parameters.add("int");
    parameters.add("3");
    parameters.add("int");
    parameters.add("int");
    parameters.add("69");

    String actualOutput =
        executeWithNamedParameters(testEnv.getPGAdapterPort(), sql, "data_type_update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types where col_bigint = %(bigint)s";
    parameters.clear();
    parameters.add("bigint");
    parameters.add("int");
    parameters.add("3");

    actualOutput =
        executeWithNamedParameters(testEnv.getPGAdapterPort(), sql, "data_type_query", parameters);
    expectedOutput =
        "(3, False, 98.6, 69, Decimal('77.46'), datetime.datetime(2022, 10, 2, 14, 34, 26, tzinfo=datetime.timezone.utc), datetime.date(2021, 12, 23), 'hello psycopg2')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void test4DeleteParametrized() throws IOException, InterruptedException {
    String sql = "DELETE FROM all_types where col_varchar = %s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("hello psycopg2");
    String actualOutput =
        executeWithParameters(testEnv.getPGAdapterPort(), sql, "update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql = "Select COUNT(*) from all_types where col_bigint = %(bigint)s";
    parameters.clear();
    parameters.add("bigint");
    parameters.add("int");
    parameters.add("3");

    actualOutput =
        executeWithNamedParameters(testEnv.getPGAdapterPort(), sql, "data_type_query", parameters);
    expectedOutput = "(0,)\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
