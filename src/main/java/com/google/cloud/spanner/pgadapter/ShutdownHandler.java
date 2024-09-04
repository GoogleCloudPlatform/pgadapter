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

import com.google.cloud.spanner.pgadapter.ProxyServer.ShutdownMode;
import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * Handles the orderly shutdown of a {@link ProxyServer}. A {@link ShutdownHandler} can only be used
 * once.
 */
public class ShutdownHandler {
  private final ProxyServer proxyServer;
  private final Thread shutdownThread;
  private final AtomicReference<ShutdownMode> shutdownMode = new AtomicReference<>();

  static ShutdownHandler createForServer(@Nonnull ProxyServer proxyServer) {
    return new ShutdownHandler(proxyServer);
  }

  /** Constructor for a {@link ShutdownHandler} for a given {@link ProxyServer}. */
  private ShutdownHandler(@Nonnull ProxyServer proxyServer) {
    this.proxyServer = Preconditions.checkNotNull(proxyServer);
    this.shutdownThread =
        new Thread("server-shutdown-handler") {
          @Override
          public void run() {
            ShutdownHandler.this.proxyServer.stopServer(ShutdownHandler.this.shutdownMode.get());
          }
        };
  }

  /** Shuts down the proxy server using the given {@link ShutdownMode}. */
  public synchronized void shutdown(@Nonnull ShutdownMode shutdownMode) {
    if (this.shutdownMode.get() != null) {
      if (this.shutdownMode.get() == ShutdownMode.SMART
          && (shutdownMode == ShutdownMode.FAST || shutdownMode == ShutdownMode.IMMEDIATE)) {
        // Upgrade shutdown mode and interrupt the shutdown thread.
        this.proxyServer.setShutdownMode(shutdownMode);
        this.shutdownThread.interrupt();
      }
      return;
    }
    this.shutdownMode.set(Preconditions.checkNotNull(shutdownMode));
    this.shutdownThread.start();
  }
}
