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

package com.google.cloud.pgadapter.tpcc;

import io.grpc.Server;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.boot.SpringApplication;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class BenchmarkApplicationTest {

  interface Server {
    int getPort();

    void shutdown();
  }

  static class PGAdapterWithEmulator implements Server {
    private final GenericContainer<?> container;

    PGAdapterWithEmulator(GenericContainer<?> container) {
      this.container = container;
    }

    @Override
    public int getPort() {
      return container.getMappedPort(5432);
    }

    @Override
    public void shutdown() {
      container.stop();
    }
  }

  /** Starts a Docker container that contains both PGAdapter and the Cloud Spanner Emulator. */
  static Server startPGAdapterWithEmulator() {
    GenericContainer<?> container =
        new GenericContainer<>(
            DockerImageName.parse("gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator"));
    container.addExposedPort(5432);
    container.setWaitStrategy(Wait.forListeningPorts(5432));
    container.start();

    return new PGAdapterWithEmulator(container);
  }

  @Test
  public void testRunApplication() throws Exception {
    // Start a Docker container with the Cloud Spanner Emulator.
    // The server will be started on a random available port.
    Server server = startPGAdapterWithEmulator();

    try {
      System.setProperty("tpcc.benchmark-duration", "PT10s");
      System.setProperty("tpcc.warehouses", "1");
      System.setProperty("tpcc.benchmark-threads", "1");
      System.setProperty("tpcc.load-data", "true");
      System.setProperty("tpcc.truncate-before-load", "false");
      System.setProperty("tpcc.run-benchmark", "false");
      System.setProperty("tpcc.benchmark-runner", "pgadapter");
      System.setProperty("tpcc.use-read-only-transactions", "true");
      System.setProperty("tpcc.lock-scanned-ranges", "false");
      System.setProperty("spanner.project", "test-project");
      System.setProperty("spanner.instance", "test-instance");
      System.setProperty("spanner.database", "tpcc");
      System.setProperty("pgadapter.in-process", "false");
      System.setProperty("pgadapter.port", String.valueOf(server.getPort()));
      System.setProperty("pgadapter.enable-open-telemetry", "false");
      System.setProperty("pgadapter.enable-open-telemetry-metrics", "false");
      System.setProperty("pgadapter.credentials", "");
      SpringApplication.run(BenchmarkApplication.class).close();
    } finally {
      server.shutdown();
    }
  }
}
