// Copyright 2026 Google LLC
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that PGAdapter shuts itself and the tool started by --command down when it is terminated.
 *
 * <p>These tests start PGAdapter in a separate JVM, because the behaviour under test only exists
 * across a process boundary: signal disposition is per-process, and the client tool is a child
 * process that outlives the JVM unless it is stopped explicitly.
 */
@RunWith(JUnit4.class)
public class ServerShutdownTest {
  /** SIGTERM must stop the server well within this, so that a hang fails instead of blocking. */
  private static final long EXIT_TIMEOUT_SECONDS = 60L;

  @Rule public Timeout globalTimeout = Timeout.seconds(300L);
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /**
   * Terminating PGAdapter must stop the tool it started. The tool is a separate process, so killing
   * the JVM does not kill it: before this was handled it was left running and reparented to init.
   */
  @Test
  public void testTermStopsTheClientTool() throws Exception {
    assumeFalse(isWindows());

    File pidFile = new File(folder.getRoot(), "client.pid");
    // exec so that the process PGAdapter starts is the long-running command itself, which is what
    // psql looks like in practice. Otherwise only the wrapping shell would be stopped.
    File tool =
        writeScript("tool.sh", "echo $$ > " + pidFile.getAbsolutePath() + "\nexec sleep 300\n");

    Process server = startServer("--command", tool.getAbsolutePath());
    try {
      String clientPid = awaitContents(pidFile);
      server.destroy(); // SIGTERM

      assertTrue(
          "PGAdapter must exit on TERM", server.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      // 143 (128 + SIGTERM) is the JVM default for TERM. Registering a TERM handler would replace
      // it, so this also asserts that --command mode leaves the default in place.
      assertEquals(143, server.exitValue());
      assertFalse("The client tool must not outlive PGAdapter", awaitProcessGone(clientPid));
    } finally {
      server.destroyForcibly();
    }
  }

  /**
   * The proxy holds a non-daemon thread, so it must be stopped even when the command could not be
   * started. Otherwise the JVM never exits.
   */
  @Test
  public void testExitsWhenCommandCannotBeStarted() throws Exception {
    assumeFalse(isWindows());

    Process server = startServer("--command", "/nonexistent/binary");
    try {
      assertTrue(
          "PGAdapter must exit when the command cannot be started",
          server.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      // A shell wrapper can only detect the failure if this is not zero.
      assertEquals(127, server.exitValue());
    } finally {
      server.destroyForcibly();
    }
  }

  /** The exit code of the client tool is what a script calling PGAdapter needs to see. */
  @Test
  public void testExitCodeOfClientToolIsPropagated() throws Exception {
    assumeFalse(isWindows());

    File tool = writeScript("tool.sh", "exit 3\n");

    Process server = startServer("--command", tool.getAbsolutePath());
    try {
      assertTrue(server.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertEquals(3, server.exitValue());
    } finally {
      server.destroyForcibly();
    }
  }

  /** Guards the standalone path: TERM must still shut down gracefully rather than exiting 143. */
  @Test
  public void testTermShutsDownGracefullyWhenRunningStandalone() throws Exception {
    assumeFalse(isWindows());

    File log = new File(folder.getRoot(), "server.log");
    Process server = startServer(log);
    try {
      awaitLogContains(log, "Server started on port");
      server.destroy(); // SIGTERM

      assertTrue(
          "PGAdapter must exit on TERM", server.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertEquals("A registered shutdown handler must shut down cleanly", 0, server.exitValue());
    } finally {
      server.destroyForcibly();
    }
  }

  /**
   * Applications that embed PGAdapter must keep their own signal handling. Creating a ProxyServer
   * initializes Server, so anything Server does at class initialization affects the whole host.
   */
  @Test
  public void testEmbeddingDoesNotChangeSignalHandling() throws Exception {
    assumeFalse(isWindows());

    assertEquals("Ctrl-C must still stop the host application", 130, runEmbeddedHost("INT"));
    assertEquals("TERM must still stop the host application", 143, runEmbeddedHost("TERM"));
  }

  /**
   * Starts a JVM that only loads PGAdapter, sends it the given signal and returns its exit code.
   */
  private int runEmbeddedHost(String signal) throws Exception {
    File pidFile = new File(folder.getRoot(), "host-" + signal + ".pid");
    Process host =
        new ProcessBuilder(
                javaExecutable(),
                "-cp",
                System.getProperty("java.class.path"),
                EmbeddedHost.class.getName(),
                pidFile.getAbsolutePath(),
                credentialsFile().getAbsolutePath())
            .redirectErrorStream(true)
            .redirectOutput(new File(folder.getRoot(), "host-" + signal + ".log"))
            .start();
    try {
      // Signals sent through the shell, because Process only offers TERM and KILL.
      new ProcessBuilder("sh", "-c", "kill -" + signal + " " + awaitContents(pidFile))
          .start()
          .waitFor();
      assertTrue(
          "The host application must exit on " + signal,
          host.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      return host.exitValue();
    } finally {
      host.destroyForcibly();
    }
  }

  /** Stands in for an application that embeds PGAdapter instead of running it as a server. */
  public static class EmbeddedHost {
    public static void main(String[] args) throws Exception {
      // The embedding path from the README. It initializes Server, because ProxyServer uses it to
      // set up OpenTelemetry.
      new ProxyServer(
              OptionsMetadata.newBuilder()
                  .setProject("p")
                  .setInstance("i")
                  .setDatabase("d")
                  .setCredentialsFile(args[1])
                  .setPort(0)
                  .build())
          .startServer();
      // Process.pid() is Java 9, and this test also runs on Java 8.
      String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
      Files.asCharSink(new File(args[0]), Charsets.UTF_8).write(pid);
      Thread.sleep(TimeUnit.MINUTES.toMillis(10L));
    }
  }

  private Process startServer(String... extraArgs) throws IOException {
    return startServer(new File(folder.getRoot(), "server.log"), extraArgs);
  }

  /** Starts PGAdapter in its own JVM on a random port. It never connects to Spanner. */
  private Process startServer(File log, String... extraArgs) throws IOException {
    List<String> command = new ArrayList<>();
    command.add(javaExecutable());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(Server.class.getName());
    Collections.addAll(command, "-p", "p", "-i", "i", "-d", "d", "-s", "0");
    Collections.addAll(command, "-c", credentialsFile().getAbsolutePath());
    Collections.addAll(command, extraArgs);

    return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log).start();
  }

  /**
   * PGAdapter refuses to start without credentials, and machines that run these tests do not
   * necessarily have application default credentials. The file is never read, because these tests
   * never connect a client to Spanner.
   */
  private File credentialsFile() throws IOException {
    File file = new File(folder.getRoot(), "credentials.json");
    if (!file.exists()) {
      Files.asCharSink(file, Charsets.UTF_8).write("{}");
    }
    return file;
  }

  private static String javaExecutable() {
    return new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
  }

  private File writeScript(String name, String body) throws IOException {
    File script = folder.newFile(name);
    Files.asCharSink(script, Charsets.UTF_8).write("#!/bin/sh\n" + body);
    assertTrue(script.setExecutable(true));
    return script;
  }

  private String awaitContents(File file) throws Exception {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(EXIT_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadline) {
      if (file.length() > 0L) {
        String contents = Files.asCharSource(file, Charsets.UTF_8).read().trim();
        if (!contents.isEmpty()) {
          return contents;
        }
      }
      Thread.sleep(100L);
    }
    throw new AssertionError(withServerOutput("Timed out waiting for " + file + " to be written"));
  }

  private void awaitLogContains(File log, String expected) throws Exception {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(EXIT_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadline) {
      if (log.exists() && Files.asCharSource(log, Charsets.UTF_8).read().contains(expected)) {
        return;
      }
      Thread.sleep(100L);
    }
    throw new AssertionError(
        withServerOutput("Timed out waiting for '" + expected + "' in " + log));
  }

  /** A timeout is almost always caused by the forked JVM failing, so report what it printed. */
  private String withServerOutput(String message) {
    StringBuilder builder = new StringBuilder(message);
    File[] files = folder.getRoot().listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(".log")) {
          builder.append("\n--- ").append(file.getName()).append(" ---\n");
          try {
            builder.append(Files.asCharSource(file, Charsets.UTF_8).read());
          } catch (IOException exception) {
            builder.append("could not be read: ").append(exception);
          }
        }
      }
    }
    return builder.toString();
  }

  /** Returns whether the process is still running, after giving it a moment to be cleaned up. */
  private static boolean awaitProcessGone(String pid) throws Exception {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
    while (System.currentTimeMillis() < deadline) {
      if (!isProcessAlive(pid)) {
        return false;
      }
      Thread.sleep(100L);
    }
    return isProcessAlive(pid);
  }

  private static boolean isProcessAlive(String pid) throws Exception {
    Process process =
        new ProcessBuilder("ps", "-p", pid)
            .redirectErrorStream(true)
            .redirectOutput(new File("/dev/null"))
            .start();
    return process.waitFor() == 0;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase().startsWith("windows");
  }
}
