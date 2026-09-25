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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
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
 * Tests how {@link SpannerPGConnector} reacts to signals. These tests start the connector in a
 * separate JVM, because signal disposition is per-process and the client tool is a child process.
 */
@RunWith(JUnit4.class)
public class SpannerPGConnectorShutdownTest {
  private static final long EXIT_TIMEOUT_SECONDS = 60L;

  @Rule public Timeout globalTimeout = Timeout.seconds(300L);
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /** Terminating the connector must stop the tool it started, rather than leaving it running. */
  @Test
  public void testTermStopsTheClientTool() throws Exception {
    assumeFalse(isWindows());

    File pidFile = new File(folder.getRoot(), "client.pid");
    // exec so that the tracked process is the long-running command itself, as it is for psql.
    File tool =
        writeScript("tool.sh", "echo $$ > " + pidFile.getAbsolutePath() + "\nexec sleep 300\n");

    Process connector = startConnector(tool.getAbsolutePath());
    try {
      String clientPid = awaitContents(pidFile);
      Thread.sleep(200L);
      connector.destroy(); // SIGTERM

      assertTrue(
          "The connector must exit on TERM",
          connector.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertEquals(143, connector.exitValue());
      assertFalse("The client tool must not outlive the connector", awaitProcessGone(clientPid));
    } finally {
      connector.destroyForcibly();
    }
  }

  /**
   * Ctrl+C is delivered to the whole foreground process group, so the client tool receives it too
   * and decides what to do (psql cancels the running query). The connector must not exit and take
   * the tool down with it.
   */
  @Test
  public void testInterruptIsLeftToTheClientTool() throws Exception {
    assumeFalse(isWindows());

    File jvmPidFile = new File(folder.getRoot(), "jvm.pid");
    // The script is a direct child of the connector JVM, so its parent is the JVM itself.
    // Process#pid() is Java 9 and this also runs on Java 8, so ask the shell instead.
    File tool =
        writeScript(
            "tool.sh", "echo $PPID > " + jvmPidFile.getAbsolutePath() + "\nexec sleep 300\n");

    Process connector = startConnector(tool.getAbsolutePath());
    try {
      signal("INT", awaitContents(jvmPidFile));

      assertFalse("The connector must not exit on INT", connector.waitFor(5L, TimeUnit.SECONDS));
    } finally {
      connector.destroyForcibly();
    }
  }

  private Process startConnector(String... args) throws IOException {
    List<String> command = new ArrayList<>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getPath());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(SpannerPGConnector.class.getName());
    Collections.addAll(command, args);

    return new ProcessBuilder(command)
        .redirectErrorStream(true)
        .redirectOutput(new File(folder.getRoot(), "connector.log"))
        .start();
  }

  /** Sends a signal that {@link Process} cannot send itself. */
  private static void signal(String signal, String pid) throws Exception {
    assertEquals(
        0, new ProcessBuilder("sh", "-c", "kill -" + signal + " " + pid).start().waitFor());
  }

  private File writeScript(String name, String body) throws IOException {
    File script = folder.newFile(name);
    Files.asCharSink(script, Charsets.UTF_8).write("#!/bin/sh\n" + body);
    assertTrue(script.setExecutable(true));
    return script;
  }

  private static String awaitContents(File file) throws Exception {
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
    throw new AssertionError("Timed out waiting for " + file + " to be written");
  }

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
    return new ProcessBuilder("ps", "-p", pid)
            .redirectErrorStream(true)
            .redirectOutput(new File("/dev/null"))
            .start()
            .waitFor()
        == 0;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase().startsWith("windows");
  }
}
