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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.protobuf.AbstractMessage;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.io.IOException;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests PGAdapter using gorm. The Go code can be found in
 * src/test/golang/pgadapter_gorm_tests/gorm.go.
 */
@Category(GolangTest.class)
@RunWith(Parameterized.class)
public class GormMigrationsMockServerTest extends AbstractMockServerTest {
  private static GormTest gormTest;

  @Parameter public boolean useDomainSocket;

  @Parameters(name = "useDomainSocket = {0}")
  public static Object[] data() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    return options.isDomainSocketEnabled() ? new Object[] {true, false} : new Object[] {false};
  }

  @BeforeClass
  public static void compile() throws IOException, InterruptedException {
    gormTest = GolangTest.compile("pgadapter_gorm_tests/gorm.go", GormTest.class);
  }

  private GoString createConnString() {
    if (useDomainSocket) {
      return new GoString(String.format("host=/tmp port=%d", pgServer.getLocalPort()));
    }
    return new GoString(
        String.format("postgres://uid:pwd@localhost:%d/?sslmode=disable", pgServer.getLocalPort()));
  }

  @Test
  public void testMigrateUser() {
    addDdlResponseToSpannerAdmin();
    String createStatement =
        "CREATE TABLE \"users\" ("
            + "\"id\" bigint,"
            + "\"name\" text,"
            + "\"email\" text,"
            + "\"age\" smallint,"
            + "\"birthday\" timestamptz,"
            + "\"member_number\" text,"
            + "\"activated_at\" timestamptz,"
            + "\"created_at\" timestamptz,"
            + "\"updated_at\" timestamptz,"
            + "PRIMARY KEY (\"id\"))";
    // TODO: CURRENT_SCHEMA() is not supported in Cloud Spanner.
    String sql =
        "SELECT count(*) "
            + "FROM information_schema.tables "
            + "WHERE table_schema = CURRENT_SCHEMA() "
            + "AND table_name = $1 "
            + "AND table_type = $2";
    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), SELECT0_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(sql).bind("p1").to("users").bind("p2").to("BASE TABLE").build(),
            SELECT0_RESULTSET));

    String res = gormTest.TestMigrateUser(createConnString());

    assertNull(res);

    List<AbstractMessage> requests = mockDatabaseAdmin.getRequests();
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest updateDatabaseDdlRequest = (UpdateDatabaseDdlRequest) requests.get(0);
    assertEquals(1, updateDatabaseDdlRequest.getStatementsCount());
    assertEquals(createStatement, updateDatabaseDdlRequest.getStatements(0));
  }
}
