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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(NodeJSTest.class)
@RunWith(JUnit4.class)
public class NodePostgresMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies("node-postgres");
  }

  @Test
  public void testSelect1() throws Exception {
    String sql = "SELECT 1";

    String output = runTest("testSelect1");

    assertEquals("\n\nSELECT 1 returned: 1\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasSingleUse());
    assertTrue(request.getTransaction().getSingleUse().hasReadOnly());
  }

  @Test
  public void testInsert() throws Exception {
    String sql = "INSERT INTO users(name) VALUES($1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    String output = runTest("testInsert");

    assertEquals("\n\nInserted 1 row(s)\n", output);

    List<ExecuteSqlRequest> executeSqlRequests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(sql))
            .collect(Collectors.toList());
    assertEquals(1, executeSqlRequests.size());
    ExecuteSqlRequest request = executeSqlRequests.get(0);
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  static String runTest(String testName) throws IOException, InterruptedException {
    return NodeJSTest.runTest("node-postgres", testName, pgServer.getLocalPort());
  }
}
