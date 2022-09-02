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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.pgadapter.IntegrationTest;
import com.google.cloud.spanner.pgadapter.PgAdapterTestEnv;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
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

  @Parameter public String host;

  @Parameters(name = "host = {0}")
  public static Object[] data() {
    return new Object[] {"localhost", "/tmp"};
  }

  @BeforeClass
  public static void setup() throws Exception {
    if (!isPythonAvailable()
        && !System.getProperty("allowSkipUnsupportedEcosystemTest", "false")
            .equalsIgnoreCase("true")) {
      throw new IllegalStateException(
          "python has not been set up on this system and allowSkipUnsupportedEcosystemTest has not been set");
    }

    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServerWithDefaultDatabase(database.getId(), Collections.emptyList());
  }

  @Before
  public void insertTestData() {
    String databaseId = database.getId().getDatabase();
    testEnv.write(
        databaseId,
        Collections.singletonList(
            Mutation.newInsertOrUpdateBuilder("all_types")
                .set("col_bigint")
                .to(1L)
                .set("col_bool")
                .to(true)
                .set("col_bytea")
                .to(ByteArray.copyFrom("test"))
                .set("col_float8")
                .to(3.14d)
                .set("col_int")
                .to(100)
                .set("col_numeric")
                .to(new BigDecimal("6.626"))
                .set("col_timestamptz")
                .to(Timestamp.parseTimestamp("2022-02-16T14:18:02+01:00"))
                .set("col_date")
                .to(Date.parseDate("2022-03-29"))
                .set("col_varchar")
                .to("test")
                .build()));
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Test
  public void testInsert() throws IOException, InterruptedException {
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

    String actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testSelect() throws IOException, InterruptedException {
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
            + "from all_types where col_bigint = 1";
    String actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "query");
    String expectedOutput =
        "(1, True, 3.14, 100, Decimal('6.626'), datetime.datetime(2022, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), datetime.date(2022, 3, 29), 'test')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testUpdate() throws IOException, InterruptedException {
    String sql =
        "UPDATE all_types SET "
            + "col_bool = 'off',"
            + "col_float8 = 3.14, "
            + "col_int = 20, "
            + "col_numeric = 99.99, "
            + "col_timestamptz = '2019-05-12 14:30:00'::timestamptz, "
            + "col_date = '1989-09-02', "
            + "col_varchar = 'new string' "
            + "where col_bigint = 1";

    String actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "update");
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
            + "from all_types where col_bigint = 1";

    actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "query");
    expectedOutput =
        "(1, False, 3.14, 20, Decimal('99.99'), datetime.datetime(2019, 5, 12, 21, 30, tzinfo=datetime.timezone.utc), datetime.date(1989, 9, 2), 'new string')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    String sql = "DELETE FROM all_types where col_bigint = 1";

    String actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "update");
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql = "Select COUNT(*) from all_types where col_bigint = 1";

    actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), sql, "query");
    expectedOutput = "(0,)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testInsertParameterized() throws IOException, InterruptedException {
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
        executeWithNamedParameters(
            host, testEnv.getPGAdapterPort(), sql, "data_type_update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testSelectParameterized() throws IOException, InterruptedException {
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
    parameters.add("test");

    String actualOutput =
        executeWithParameters(host, testEnv.getPGAdapterPort(), sql, "query", parameters);
    String expectedOutput =
        "(1, True, 3.14, 100, Decimal('6.626'), datetime.datetime(2022, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), datetime.date(2022, 3, 29), 'test')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testUpdateParameterized() throws IOException, InterruptedException {
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
    parameters.add("1");
    parameters.add("int");
    parameters.add("int");
    parameters.add("69");

    String actualOutput =
        executeWithNamedParameters(
            host, testEnv.getPGAdapterPort(), sql, "data_type_update", parameters);
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
    parameters.add("1");

    actualOutput =
        executeWithNamedParameters(
            host, testEnv.getPGAdapterPort(), sql, "data_type_query", parameters);
    expectedOutput =
        "(1, False, 98.6, 69, Decimal('77.46'), datetime.datetime(2022, 10, 2, 14, 34, 26, tzinfo=datetime.timezone.utc), datetime.date(2021, 12, 23), 'hello psycopg2')\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testDeleteParameterized() throws IOException, InterruptedException {
    String sql = "DELETE FROM all_types where col_varchar = %s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("test");
    String actualOutput =
        executeWithParameters(host, testEnv.getPGAdapterPort(), sql, "update", parameters);
    String expectedOutput = "1\n";
    assertEquals(expectedOutput, actualOutput);

    sql = "Select COUNT(*) from all_types where col_bigint = %(bigint)s";
    parameters.clear();
    parameters.add("bigint");
    parameters.add("int");
    parameters.add("1");

    actualOutput =
        executeWithNamedParameters(
            host, testEnv.getPGAdapterPort(), sql, "data_type_query", parameters);
    expectedOutput = "(0,)\n";
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testInsertUsingExecuteMany() throws IOException, InterruptedException {
    String insertSql =
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
            + "%(varchar)s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("9");
    parameters.add("bool");
    parameters.add("True");
    parameters.add("float");
    parameters.add("89.23");
    parameters.add("numeric");
    parameters.add("33.546");
    parameters.add("tstz");
    parameters.add("2022-02-16T14:18:02+01:00");
    parameters.add("varchar");
    parameters.add("hello world");
    parameters.add("date");
    parameters.add("2022-7-1");
    parameters.add("bigint");
    parameters.add("10");
    parameters.add("bytea");
    parameters.add("alpha beta");
    parameters.add("int");
    parameters.add("34");

    parameters.add("bool");
    parameters.add("False");
    parameters.add("float");
    parameters.add("74.567");
    parameters.add("numeric");
    parameters.add("99.9999");
    parameters.add("tstz");
    parameters.add("2021-02-16T14:18:02+01:00");
    parameters.add("varchar");
    parameters.add("bye bye world");
    parameters.add("date");
    parameters.add("2021-7-1");
    parameters.add("bigint");
    parameters.add("15");
    parameters.add("bytea");
    parameters.add("gamma theta");
    parameters.add("int");
    parameters.add("69");

    String actualOutput =
        executeInBatch(
            host, testEnv.getPGAdapterPort(), insertSql, "named_execute_many", parameters);
    String expectedOutput = "2\n";

    assertEquals(expectedOutput, actualOutput);

    String selectSql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types "
            + "where col_bigint = 10 or col_bigint = 15 "
            + "order by col_bigint";

    expectedOutput =
        "(10, "
            + "True, "
            + "89.23, "
            + "34, "
            + "Decimal('33.546'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 7, 1), "
            + "'hello world')"
            + "\n"
            + "(15, "
            + "False, "
            + "74.567, "
            + "69, "
            + "Decimal('99.9999'), "
            + "datetime.datetime(2021, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2021, 7, 1), "
            + "'bye bye world')"
            + "\n";

    actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), selectSql, "query");
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testInsertUsingExecuteBatch() throws IOException, InterruptedException {
    String insertSql =
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
            + "%(varchar)s)";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("9");
    parameters.add("bool");
    parameters.add("True");
    parameters.add("float");
    parameters.add("89.23");
    parameters.add("numeric");
    parameters.add("33.546");
    parameters.add("tstz");
    parameters.add("2022-02-16T14:18:02+01:00");
    parameters.add("varchar");
    parameters.add("hello world");
    parameters.add("date");
    parameters.add("2022-7-1");
    parameters.add("bigint");
    parameters.add("11");
    parameters.add("bytea");
    parameters.add("alpha beta");
    parameters.add("int");
    parameters.add("34");

    parameters.add("bool");
    parameters.add("False");
    parameters.add("float");
    parameters.add("74.567");
    parameters.add("numeric");
    parameters.add("99.9999");
    parameters.add("tstz");
    parameters.add("2021-02-16T14:18:02+01:00");
    parameters.add("varchar");
    parameters.add("bye bye world");
    parameters.add("date");
    parameters.add("2021-7-1");
    parameters.add("bigint");
    parameters.add("16");
    parameters.add("bytea");
    parameters.add("gamma theta");
    parameters.add("int");
    parameters.add("69");

    String actualOutput =
        executeInBatch(
            host, testEnv.getPGAdapterPort(), insertSql, "named_execute_batch", parameters);
    String expectedOutput = "1\n";

    assertEquals(expectedOutput, actualOutput);

    String selectSql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types "
            + "where col_bigint = 11 or col_bigint = 16 "
            + "order by col_bigint";

    expectedOutput =
        "(11, "
            + "True, "
            + "89.23, "
            + "34, "
            + "Decimal('33.546'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 7, 1), "
            + "'hello world')"
            + "\n"
            + "(16, "
            + "False, "
            + "74.567, "
            + "69, "
            + "Decimal('99.9999'), "
            + "datetime.datetime(2021, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2021, 7, 1), "
            + "'bye bye world')"
            + "\n";

    actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), selectSql, "query");
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testInsertUsingExecuteValues() throws IOException, InterruptedException {
    String insertSql =
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
            + " values %s";

    ArrayList<String> parameters = new ArrayList<>();
    parameters.add("9");

    parameters.add("12");
    parameters.add("True");
    parameters.add("alpha beta");
    parameters.add("89.23");
    parameters.add("34");
    parameters.add("33.546");
    parameters.add("2022-02-16T14:18:02+01:00");
    parameters.add("2022-7-1");
    parameters.add("hello world");

    parameters.add("17");
    parameters.add("False");
    parameters.add("gamma theta");
    parameters.add("74.567");
    parameters.add("69");
    parameters.add("99.9999");
    parameters.add("2021-02-16T14:18:02+01:00");
    parameters.add("2021-7-1");
    parameters.add("bye bye world");

    String actualOutput =
        executeInBatch(host, testEnv.getPGAdapterPort(), insertSql, "execute_values", parameters);
    String expectedOutput = "2\n";

    assertEquals(expectedOutput, actualOutput);

    String selectSql =
        "Select "
            + "col_bigint, "
            + "col_bool , "
            + "col_float8 , "
            + "col_int , "
            + "col_numeric , "
            + "col_timestamptz , "
            + "col_date , "
            + "col_varchar "
            + "from all_types "
            + "where col_bigint = 12 or col_bigint = 17 "
            + "order by col_bigint";

    expectedOutput =
        "(12, "
            + "True, "
            + "89.23, "
            + "34, "
            + "Decimal('33.546'), "
            + "datetime.datetime(2022, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2022, 7, 1), "
            + "'hello world')"
            + "\n"
            + "(17, "
            + "False, "
            + "74.567, "
            + "69, "
            + "Decimal('99.9999'), "
            + "datetime.datetime(2021, 2, 16, 13, 18, 2, tzinfo=datetime.timezone.utc), "
            + "datetime.date(2021, 7, 1), "
            + "'bye bye world')"
            + "\n";

    actualOutput = executeWithoutParameters(host, testEnv.getPGAdapterPort(), selectSql, "query");
    assertEquals(expectedOutput, actualOutput);
  }
}
