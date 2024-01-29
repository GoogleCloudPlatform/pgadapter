// Copyright 2024 Google LLC
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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {
  private static final List<String> IDENTIFIERS = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      throw new IllegalArgumentException("Give 3 args");
    }
    int numWorkers = Integer.parseInt(args[0]);
    int iterations = Integer.parseInt(args[1]);
    boolean runClientLibrary = "client".equalsIgnoreCase(args[2]);

    System.out.println("");
    System.out.println(" /----------------------------------------/ ");
    System.out.printf(
        "Running benchmark with %d workers, %d iterations on %s\n",
        numWorkers, iterations, runClientLibrary ? "Java Client" : "PGAdapter");
    System.out.println("");
    System.out.println("");

    OptionsMetadata options =
        OptionsMetadata.newBuilder()
            .setProject("appdev-soda-spanner-staging")
            .setInstance("knut-test-ycsb")
            .setCredentialsFile("/home/loite/appdev-soda-spanner-staging.json")
            .build();
    ProxyServer server = new ProxyServer(options);
    server.startServer();
    server.awaitRunning();

    Spanner spanner =
        SpannerOptions.newBuilder()
            .setProjectId("appdev-soda-spanner-staging")
            .setCredentials(
                GoogleCredentials.fromStream(
                    Files.newInputStream(
                        new File("/home/loite/appdev-soda-spanner-staging.json").toPath())))
            .build()
            .getService();

    try (Connection connection =
        DriverManager.getConnection(
            String.format("jdbc:postgresql://localhost:%d/knut-test-db", server.getLocalPort()))) {
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select id from benchmark_all_types")) {
        while (resultSet.next()) {
          IDENTIFIERS.add(resultSet.getString(1));
        }
      }
    }
    int numOperations = numWorkers * iterations;
    AtomicInteger counter = new AtomicInteger();
    Stopwatch watch = Stopwatch.createStarted();
    ExecutorService service = Executors.newFixedThreadPool(numWorkers);
    for (int i = 0; i < numWorkers; i++) {
      service.submit(
          () -> {
            if (runClientLibrary) {
              try {
                DatabaseClient client =
                    spanner.getDatabaseClient(
                        DatabaseId.of(
                            "appdev-soda-spanner-staging", "knut-test-ycsb", "knut-test-db"));
                Statement statement =
                    Statement.newBuilder("select * from benchmark_all_types where id=$1").build();
                for (int n = 0; n < iterations; n++) {
                  try (com.google.cloud.spanner.ResultSet resultSet =
                      client
                          .singleUse()
                          .executeQuery(
                              statement
                                  .toBuilder()
                                  .bind("p1")
                                  .to(
                                      IDENTIFIERS.get(
                                          ThreadLocalRandom.current().nextInt(IDENTIFIERS.size())))
                                  .build())) {
                    while (resultSet.next()) {
                      for (int col = 0; col < resultSet.getColumnCount(); col++) {
                        resultSet.getValue(col).getAsString();
                      }
                    }
                  }
                  counter.incrementAndGet();
                }
              } catch (Throwable t) {
                t.printStackTrace();
              }
            } else {
              Uninterruptibles.sleepUninterruptibly(
                  ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
              try (Connection connection =
                  DriverManager.getConnection(
                      String.format(
                          "jdbc:postgresql://localhost:%d/knut-test-db", server.getLocalPort()))) {
                try (PreparedStatement statement =
                    connection.prepareStatement("select * from benchmark_all_types where id=?")) {
                  for (int n = 0; n < iterations; n++) {
                    statement.setString(
                        1,
                        IDENTIFIERS.get(ThreadLocalRandom.current().nextInt(IDENTIFIERS.size())));
                    try (ResultSet resultSet = statement.executeQuery()) {
                      while (resultSet.next()) {
                        for (int col = 1; col <= resultSet.getMetaData().getColumnCount(); col++) {
                          resultSet.getObject(col);
                        }
                      }
                    }
                    counter.incrementAndGet();
                  }
                }
              } catch (Throwable t) {
                t.printStackTrace();
              }
            }
          });
    }

    Thread progress =
        new Thread("progress") {
          @Override
          public void run() {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                System.out.printf("Progress: %d/%d\n", counter.get(), numOperations);
                //noinspection BusyWait
                Thread.sleep(1000L);
              } catch (InterruptedException ignore) {
                return;
              }
            }
          }
        };
    progress.setDaemon(true);
    progress.start();

    service.shutdown();
    if (!service.awaitTermination(30L, TimeUnit.MINUTES)) {
      throw new TimeoutException();
    }
    System.out.println("Total elapsed: " + watch.elapsed(TimeUnit.SECONDS));

    server.stopServer();
    server.awaitTerminated();

    spanner.close();
  }
}
