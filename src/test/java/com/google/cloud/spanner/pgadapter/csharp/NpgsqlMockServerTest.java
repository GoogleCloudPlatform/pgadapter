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

package com.google.cloud.spanner.pgadapter.csharp;

import static org.junit.Assert.assertEquals;

import com.google.spanner.v1.ExecuteSqlRequest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NpgsqlMockServerTest extends AbstractNpgsqlMockServerTest {

  private String createConnectionString() {
    return String.format(
        "Host=localhost;Port=%d;Database=d;SSL Mode=Disable", pgServer.getLocalPort());
  }

  @Test
  public void testSelect1() throws IOException, InterruptedException {
    String result = execute("TestSelect1", createConnectionString());
    assertEquals("Success\n", result);

    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(SELECT1.getSql()))
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
  }
}
