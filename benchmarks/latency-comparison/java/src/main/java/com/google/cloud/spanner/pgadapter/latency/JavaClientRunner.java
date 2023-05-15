// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.latency;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class JavaClientRunner implements BenchmarkRunner {
  private final Random random = new Random();
  private final DatabaseId databaseId;
  private long numNullValues;
  private long numNonNullValues;
  
  JavaClientRunner(DatabaseId databaseId) {
    this.databaseId = databaseId;
  }
  
  @Override
  public List<Duration> execute(String sql, int numClients, int numOperations) {
    List<Duration> results = new ArrayList<>(numOperations);
    SpannerOptions options = SpannerOptions.newBuilder()
        .setProjectId(databaseId.getInstanceId().getProject())
        .build();
    try (Spanner spanner = options.getService()) {
      DatabaseClient client = spanner.getDatabaseClient(databaseId);
      // Execute one query to make sure everything has been warmed up.
      executeQuery(client, sql);
      
      for (int i=0; i<numOperations; i++) {
        results.add(executeQuery(client, sql));
      }
    }
    
    return results;
  }
  
  private Duration executeQuery(DatabaseClient client, String sql) {
    Stopwatch watch = Stopwatch.createStarted();
    try (ResultSet resultSet = client.singleUse().executeQuery(Statement.newBuilder(sql)
            .bind("p1").to(random.nextInt(100000))
        .build())) {
      while (resultSet.next()) {
        for (int i=0; i<resultSet.getColumnCount(); i++) {
          if (resultSet.isNull(i)) {
            numNullValues++;
          } else {
            numNonNullValues++;
          }
        }
      }
    }
    return watch.elapsed();
  }

}
