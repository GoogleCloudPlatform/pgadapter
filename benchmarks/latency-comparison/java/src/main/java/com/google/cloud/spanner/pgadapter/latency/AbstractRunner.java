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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractRunner implements BenchmarkRunner {
  private final AtomicInteger operationCounter = new AtomicInteger();

  protected void incOperations() {
    operationCounter.incrementAndGet();
  }

  protected List<Duration> collectResults(
      ExecutorService service,
      List<Future<List<Duration>>> results,
      int numClients,
      int numOperations)
      throws Exception {
    int totalOperations = numClients * numOperations;
    service.shutdown();
    while (!service.isTerminated()) {
      //noinspection BusyWait
      Thread.sleep(1000L);
      System.out.printf("\r%d/%d", operationCounter.get(), totalOperations);
    }
    System.out.println();
    if (!service.awaitTermination(60L, TimeUnit.MINUTES)) {
      throw new TimeoutException();
    }
    List<Duration> allResults = new ArrayList<>(numClients * numOperations);
    for (Future<List<Duration>> result : results) {
      allResults.addAll(result.get());
    }
    return allResults;
  }

  protected void randomWait(int waitMillis) throws InterruptedException {
    if (waitMillis <= 0) {
      return;
    }
    int randomMillis = ThreadLocalRandom.current().nextInt(waitMillis * 2);
    Thread.sleep(randomMillis);
  }
}
