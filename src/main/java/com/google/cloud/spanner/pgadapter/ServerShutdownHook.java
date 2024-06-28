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

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;

class ServerShutdownHook extends Thread {

  private final ProxyServer server;

  private final ExitMode exitMode;

  ServerShutdownHook(ProxyServer server, ExitMode exitMode) {
    this.server = server;
    this.exitMode = exitMode;
  }

  @Override
  public void run() {
    try {
      if (this.server != null && this.server.isRunning()) {
        this.server.stopServer();
        this.server.awaitTerminated();
      }
      if (this.exitMode == ExitMode.HALT_WITH_EXIT_CODE_ZERO_ON_SUCCESS) {
        // Halt the system with a zero exit code if the shutdown succeeded.
        // Wait with calling halt(0) until there are only two threads alive.
        // That should be the DestroyJavaVM thread + this thread.
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (Thread.activeCount() > 2 && stopwatch.elapsed(TimeUnit.MILLISECONDS) < 100) {
          Thread.yield();
        }
        Runtime.getRuntime().halt(0);
      }
    } catch (Exception exception) {
      // Shutdown hooks cannot use java.util.logging.
      // See https://bugs.openjdk.org/browse/JDK-8161253.
      System.err.println("Server shutdown failed");
      exception.printStackTrace(System.err);
    }
  }
}
