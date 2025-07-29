// Copyright 2025 Google LLC
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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.spanner.v1.ExecuteSqlRequest;
import io.opentelemetry.api.OpenTelemetry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CommandMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void checkPsqlAvailable() {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "d",
        builder -> builder.setDdlTransactionMode(DdlTransactionMode.AutocommitExplicitTransaction),
        OpenTelemetry.noop());
  }

  private static boolean isPsqlAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  @Test
  public void testRunCommand() throws Exception {
    String sql = "update psql_test set value=1 where id=1";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 100L));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Void> future =
        executor.submit(
            () -> {
              Server.runCommand(pgServer, "d", "psql", "-c", sql);
              return null;
            });
    executor.shutdown();
    future.get();

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertEquals(sql, request.getSql());
  }
}
