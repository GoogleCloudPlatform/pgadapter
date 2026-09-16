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

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ServerTest {

  /** Static state, so tests that run a shutdown hook would otherwise affect the ones after them. */
  @After
  public void resetShutdownState() {
    Server.shuttingDown.set(false);
  }

  @Test
  public void testExtractMetadata() {
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(byteArrayStream);
    String expectedPGAdapterVersion = Server.getVersion();
    String expectedPostgreSQLVersion = new OptionsMetadata(new String[] {}).getServerVersion();

    Server.extractMetadata(new String[] {}, out);

    assertEquals(
        "-- Starting PGAdapter version "
            + expectedPGAdapterVersion
            + " --\n"
            + "-- PostgreSQL version: "
            + expectedPostgreSQLVersion
            + " -- \n",
        byteArrayStream.toString());
  }

  @Test
  public void testPrintError() {
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(byteArrayStream);
    Exception exception = new Exception("test exception");
    String expectedPGAdapterVersion = Server.getVersion();

    Server.printError(exception, out, out);

    assertEquals(
        "The server could not be started because an error occurred: "
            + exception.getMessage()
            + "\n"
            + "Run with option -h or --help to get help\n"
            + "Version: "
            + expectedPGAdapterVersion
            + "\n",
        byteArrayStream.toString());
  }

  @Test
  public void testMainWithInvalidParam() {
    ByteArrayOutputStream outArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outArrayStream);
    ByteArrayOutputStream errArrayStream = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errArrayStream);

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    System.setOut(out);
    System.setErr(err);

    try {
      assertEquals(1, Server.run(new String[] {"--invalid-param"}));
      assertEquals(
          "The server could not be started because an error occurred: Unrecognized option: --invalid-param\n",
          errArrayStream.toString());
      assertTrue(
          outArrayStream.toString(),
          outArrayStream
              .toString()
              .startsWith(
                  String.format("-- Starting PGAdapter version %s --", Server.getVersion())));
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }
  }

  @Test
  public void testInvalidKeyStore() {
    ByteArrayOutputStream outArrayStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(outArrayStream);
    ByteArrayOutputStream errArrayStream = new ByteArrayOutputStream();
    PrintStream err = new PrintStream(errArrayStream);

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    String originalKeyStore = System.getProperty("javax.net.ssl.keyStore");
    System.setOut(out);
    System.setErr(err);
    System.setProperty("javax.net.ssl.keyStore", "/path/to/non/existing/file.pfx");

    try {
      assertEquals(1, Server.run(new String[] {}));
      assertEquals(
          "The server could not be started because an error occurred: Key store /path/to/non/existing/file.pfx does not exist\n",
          errArrayStream.toString());
      assertTrue(
          outArrayStream.toString(),
          outArrayStream
              .toString()
              .startsWith(
                  String.format("-- Starting PGAdapter version %s --", Server.getVersion())));
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
      if (originalKeyStore != null) {
        System.setProperty("javax.net.ssl.keyStore", originalKeyStore);
      }
    }
  }

  @Test
  public void testStartAndWaitReturnsExitCode() throws Exception {
    assumeFalse(isWindows());

    assertEquals(0, Server.startAndWait(new ProcessBuilder("sh", "-c", "exit 0")));
    assertEquals(3, Server.startAndWait(new ProcessBuilder("sh", "-c", "exit 3")));
  }

  @Test
  public void testDestroyClientProcessIsNoOpWhenNothingIsRunning() {
    Server.destroyClientProcess();
    Server.destroyClientProcess();
  }

  /**
   * Killing this process does not kill the client tool, so the shutdown hook must stop it
   * explicitly. Verifies that a running command is actually stopped.
   */
  @Test
  public void testDestroyClientProcessStopsRunningCommand() throws Exception {
    assumeFalse(isWindows());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Integer> exitCode =
          executor.submit(() -> Server.startAndWait(new ProcessBuilder("sleep", "300")));

      // Retry rather than sleeping a fixed amount: destroyClientProcess() is a no-op until
      // startAndWait has registered the process, and that can be slow on a loaded machine.
      long deadline = System.currentTimeMillis() + 10_000L;
      while (!exitCode.isDone() && System.currentTimeMillis() < deadline) {
        Server.destroyClientProcess();
        Thread.sleep(50L);
      }

      assertNotEquals(0, exitCode.get(10L, TimeUnit.SECONDS).intValue());
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * The JVM halts as soon as the last shutdown hook returns. A hook that returned while a client
   * tool was still being started would leave that tool running.
   */
  @Test
  public void testShutdownHookWaitsWhileTheClientToolIsStarting() throws Exception {
    Thread shutdownHook = Server.createShutdownHook(mock(ProxyServer.class), "test-shutdown-hook");

    // Holding the lock stands in for being inside ProcessBuilder.start().
    synchronized (Server.clientProcessLock) {
      shutdownHook.start();
      shutdownHook.join(500L);
      assertTrue("The shutdown hook must wait for the client tool", shutdownHook.isAlive());
    }

    shutdownHook.join(10_000L);
    assertFalse(shutdownHook.isAlive());
  }

  /** Starting a tool after shutdown has looked for one would leave it running. */
  @Test
  public void testClientToolIsNotStartedWhenAlreadyShuttingDown() throws Exception {
    assumeFalse(isWindows());
    Server.createShutdownHook(mock(ProxyServer.class), "test-shutdown-hook").run();

    assertEquals(127, Server.startAndWait(new ProcessBuilder("sh", "-c", "exit 0")));
  }

  /**
   * Every caller must reach stopServer, as that is what makes them wait for the shutdown to finish.
   * Returning early instead would allow the JVM to halt while another thread is still stopping the
   * server.
   */
  @Test
  public void testStopProxyServerWaitsOnEveryCall() {
    ProxyServer proxyServer = mock(ProxyServer.class);

    Server.stopProxyServer(proxyServer);
    Server.stopProxyServer(proxyServer);

    verify(proxyServer, times(2)).stopServer();
  }

  @Test
  public void testStopProxyServerIgnoresErrors() {
    ProxyServer proxyServer = mock(ProxyServer.class);
    doThrow(new IllegalStateException("test")).when(proxyServer).stopServer();

    Server.stopProxyServer(proxyServer);
    Server.stopProxyServer(null);
  }

  @Test
  public void testRemoveShutdownHook() {
    Server.removeShutdownHook(null);

    Thread shutdownHook = new Thread(() -> {});
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    Server.removeShutdownHook(shutdownHook);
    // Removing a hook that is no longer registered must not throw.
    Server.removeShutdownHook(shutdownHook);
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase().startsWith("windows");
  }
}
