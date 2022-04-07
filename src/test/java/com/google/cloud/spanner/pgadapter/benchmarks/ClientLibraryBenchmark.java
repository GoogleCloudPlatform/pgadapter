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

package com.google.cloud.spanner.pgadapter.benchmarks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.openjdk.jmh.annotations.Benchmark;

public class ClientLibraryBenchmark extends AbstractBenchmark {

  @Benchmark
  public void testSelect1(ClientLibraryBenchmarkState client) {
    String sql = "SELECT 1";

    try (ResultSet resultSet = client.getClient().singleUse().executeQuery(Statement.of(sql))) {
      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong(0));
      assertFalse(resultSet.next());
    }
  }

  @Benchmark
  public void testSelectOneRow(ClientLibraryBenchmarkState client) {
    String sql = "select * from all_types limit 1";

    try (ResultSet resultSet = client.getClient().singleUse().executeQuery(Statement.of(sql))) {
      assertTrue(resultSet.next());
      for (int i = 0; i < resultSet.getColumnCount(); i++) {
        resultSet.getValue(i);
      }
      assertFalse(resultSet.next());
    }
  }
}
