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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ServerTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

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
      Server.main(new String[] {"--invalid-param"});
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
      Server.main(new String[] {});
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
      } else {
        System.clearProperty("javax.net.ssl.keyStore");
      }
    }
  }

  @Test
  public void testEmbeddedModeDoesNotOverrideSignalHandlers() throws Exception {
    Object originalInt = Server.registerSignalHandler("INT", "handleIgnore", Level.WARNING);
    Object originalTerm = Server.registerSignalHandler("TERM", "handleIgnore", Level.WARNING);
    assumeNotNull(originalInt, originalTerm);
    try {
      // Capture the sun.misc.SignalHandler instances installed by registerSignalHandler above,
      // then reinstall them as our known probe handlers before loading ProxyServer and Server.
      Object probeInt = Server.restoreSignalHandler("INT", originalInt);
      Object probeTerm = Server.restoreSignalHandler("TERM", originalTerm);
      Server.restoreSignalHandler("INT", probeInt);
      Server.restoreSignalHandler("TERM", probeTerm);

      File creds = folder.newFile("creds.json");
      Files.asCharSink(creds, StandardCharsets.UTF_8).write("{}");
      OptionsMetadata options =
          OptionsMetadata.newBuilder()
              .setProject("p")
              .setInstance("i")
              .setDatabase("d")
              .setCredentialsFile(creds.getAbsolutePath())
              .setPort(0)
              .build();

      // Load ProxyServer and Server in a fresh ClassLoader so that Server.<clinit> runs after
      // probeInt and probeTerm have been installed.
      ClassLoader freshLoader = new IsolatedServerClassLoader(ServerTest.class.getClassLoader());
      Class<?> freshProxyServerClass =
          Class.forName(ProxyServer.class.getName(), true, freshLoader);
      freshProxyServerClass.getConstructor(OptionsMetadata.class).newInstance(options);

      // Restoring originalInt/originalTerm returns the currently active handler. Verify that
      // probeInt/probeTerm were NOT overwritten by ProxyServer or Server.<clinit>.
      assertSame(probeInt, Server.restoreSignalHandler("INT", originalInt));
      assertSame(probeTerm, Server.restoreSignalHandler("TERM", originalTerm));
    } finally {
      Server.restoreSignalHandler("INT", originalInt);
      Server.restoreSignalHandler("TERM", originalTerm);
    }
  }

  @Test
  public void testIgnoreAndRestoreInterruptSignal() {
    Object original = Server.registerSignalHandler("INT", "handleIgnore", Level.WARNING);
    assumeNotNull(original);
    try {
      Object saved = Server.ignoreInterruptSignal();
      assertNotNull(saved);

      Server.restoreSignalHandler("INT", saved);
      Object restored = Server.registerSignalHandler("INT", "handleIgnore", Level.WARNING);
      assertSame(saved, restored);
    } finally {
      Server.restoreSignalHandler("INT", original);
    }
  }

  @Test
  public void testStopCommandDestroysProcessAndStopsServer() throws Exception {
    Process gracefulProcess = mock(Process.class);
    when(gracefulProcess.waitFor(anyLong(), any(TimeUnit.class))).thenReturn(true);
    ProxyServer proxyServer1 = mock(ProxyServer.class);

    Server.stopCommand(gracefulProcess, proxyServer1);

    verify(gracefulProcess).destroy();
    verify(gracefulProcess, never()).destroyForcibly();
    verify(proxyServer1).stopServer();

    Process hungProcess = mock(Process.class);
    when(hungProcess.waitFor(anyLong(), any(TimeUnit.class))).thenReturn(false);
    ProxyServer proxyServer2 = mock(ProxyServer.class);

    Server.stopCommand(hungProcess, proxyServer2);

    verify(hungProcess).destroy();
    verify(hungProcess).destroyForcibly();
    verify(proxyServer2).stopServer();
  }

  @Test(timeout = 60_000L)
  public void testStandaloneServerStopsGracefullyOnSigTerm() throws Exception {
    assumeFalse(isWindows());

    File creds = folder.newFile("standalone-creds.json");
    Files.asCharSink(creds, StandardCharsets.UTF_8).write("{}");
    Process server =
        new ProcessBuilder(
                new File(new File(System.getProperty("java.home"), "bin"), "java").getPath(),
                "-cp",
                System.getProperty("java.class.path"),
                Server.class.getName(),
                "-p",
                "p",
                "-i",
                "i",
                "-d",
                "d",
                "-s",
                "0",
                "-c",
                creds.getAbsolutePath())
            .redirectErrorStream(true)
            .start();

    try (BufferedReader out = new BufferedReader(new InputStreamReader(server.getInputStream()))) {
      boolean started = false;
      String line;
      while ((line = out.readLine()) != null) {
        if (line.contains("Server started on port")) {
          started = true;
          break;
        }
      }
      assertTrue("Server never reported startup", started);
      server.destroy(); // Sends SIGTERM

      assertTrue(server.waitFor(15L, TimeUnit.SECONDS));
      assertEquals(0, server.exitValue());
    } finally {
      server.destroyForcibly();
    }
  }

  @Test(timeout = 60_000L)
  public void testCommandModeIgnoresSigIntAndTerminatesChildOnSigTerm() throws Exception {
    assumeFalse(isWindows());

    File creds = folder.newFile("cmd-creds.json");
    Files.asCharSink(creds, StandardCharsets.UTF_8).write("{}");
    File childPidFile = new File(folder.getRoot(), "child.pid");
    File childIntFile = new File(folder.getRoot(), "child.int");
    File childScript = folder.newFile("child-cmd.sh");
    Files.asCharSink(childScript, StandardCharsets.UTF_8)
        .write(
            "#!/bin/sh\n"
                + "trap 'echo INT_RECEIVED > \""
                + childIntFile.getAbsolutePath()
                + "\"' INT\n"
                + "echo $$ > \""
                + childPidFile.getAbsolutePath()
                + "\"\n"
                + "echo CHILD_READY\n"
                + "while :; do\n"
                + "  sleep 1\n"
                + "done\n");
    assertTrue(childScript.setExecutable(true));

    String javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
    Process server =
        new ProcessBuilder(
                "sh",
                "-c",
                "echo SERVER_PID=$$; exec \"$@\"",
                "sh",
                javaBin,
                "-cp",
                System.getProperty("java.class.path"),
                Server.class.getName(),
                "-p",
                "p",
                "-i",
                "i",
                "-d",
                "d",
                "-s",
                "0",
                "-c",
                creds.getAbsolutePath(),
                "-cmd",
                childScript.getAbsolutePath())
            .redirectErrorStream(true)
            .start();

    String serverPid = null;
    String childPid = null;
    try (BufferedReader out = new BufferedReader(new InputStreamReader(server.getInputStream()))) {
      boolean childReady = false;
      String line;
      while ((line = out.readLine()) != null) {
        if (line.startsWith("SERVER_PID=")) {
          serverPid = line.substring("SERVER_PID=".length()).trim();
        } else if (line.equals("CHILD_READY")) {
          childReady = true;
          break;
        }
      }
      assertTrue("Child command never reported CHILD_READY", childReady);
      assertNotNull("Server PID was not captured", serverPid);
      childPid = Files.asCharSource(childPidFile, StandardCharsets.UTF_8).read().trim();

      // Simulate Ctrl+C (SIGINT) delivered to both PGAdapter and the child process.
      assertEquals(0, new ProcessBuilder("kill", "-INT", serverPid, childPid).start().waitFor());

      Stopwatch intTimer = Stopwatch.createStarted();
      while (!childIntFile.exists() && intTimer.elapsed(TimeUnit.SECONDS) < 5L) {
        Thread.sleep(50L);
      }
      assertTrue("Child process did not receive SIGINT", childIntFile.exists());
      assertTrue("PGAdapter in --cmd mode must ignore SIGINT while child runs", server.isAlive());

      // Now send SIGTERM to PGAdapter and verify both PGAdapter and the child process terminate.
      server.destroy();
      assertTrue(server.waitFor(15L, TimeUnit.SECONDS));
      assertEquals(143, server.exitValue());

      Stopwatch childExitTimer = Stopwatch.createStarted();
      while (new ProcessBuilder("kill", "-0", childPid).start().waitFor() == 0
          && childExitTimer.elapsed(TimeUnit.SECONDS) < 5L) {
        Thread.sleep(50L);
      }
      assertTrue(
          "Child process should be terminated by PGAdapter shutdown hook",
          new ProcessBuilder("kill", "-0", childPid).start().waitFor() != 0);
    } finally {
      server.destroyForcibly();
      if (childPid != null) {
        new ProcessBuilder("kill", "-9", childPid).start().waitFor();
      }
    }
  }

  @Test(timeout = 60_000L)
  public void testEmbeddedHostHandlesSigTermAndRunsShutdownHooks() throws Exception {
    assumeFalse(isWindows());

    File creds = folder.newFile("embedded-creds.json");
    Files.asCharSink(creds, StandardCharsets.UTF_8).write("{}");
    File hookFile = folder.newFile("hook.txt");
    Process host =
        new ProcessBuilder(
                new File(new File(System.getProperty("java.home"), "bin"), "java").getPath(),
                "-cp",
                System.getProperty("java.class.path"),
                EmbeddedHost.class.getName(),
                creds.getAbsolutePath(),
                hookFile.getAbsolutePath())
            .redirectErrorStream(true)
            .start();

    try (BufferedReader out = new BufferedReader(new InputStreamReader(host.getInputStream()))) {
      boolean ready = false;
      String line;
      while ((line = out.readLine()) != null) {
        if (line.equals("READY")) {
          ready = true;
          break;
        }
      }
      assertTrue("EmbeddedHost never reported READY", ready);
      host.destroy(); // Sends SIGTERM

      assertTrue(host.waitFor(15L, TimeUnit.SECONDS));
      assertEquals(143, host.exitValue());
      assertEquals("HOOK_RAN", Files.asCharSource(hookFile, StandardCharsets.UTF_8).read());
    } finally {
      host.destroyForcibly();
    }
  }

  public static class EmbeddedHost {
    public static void main(String[] args) throws Exception {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      Files.asCharSink(new File(args[1]), StandardCharsets.UTF_8).write("HOOK_RAN");
                    } catch (IOException ignored) {
                      // ignore
                    }
                  }));
      new ProxyServer(
              OptionsMetadata.newBuilder()
                  .setProject("p")
                  .setInstance("i")
                  .setDatabase("d")
                  .setCredentialsFile(args[0])
                  .setPort(0)
                  .build())
          .startServer();
      System.out.println("READY");
      System.out.flush();
      Thread.sleep(TimeUnit.MINUTES.toMillis(10L));
    }
  }

  /**
   * Child-first {@link ClassLoader} for {@link Server} and {@link ProxyServer} that forces their
   * static initializers ({@code <clinit>}) to execute in-process during testing even if {@link
   * Server} was already initialized by the parent test classloader.
   */
  private static final class IsolatedServerClassLoader extends ClassLoader {
    IsolatedServerClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          if (name.startsWith(Server.class.getName())
              || name.startsWith(ProxyServer.class.getName())) {
            String path = name.replace('.', '/') + ".class";
            try (InputStream in = getParent().getResourceAsStream(path)) {
              if (in == null) {
                throw new ClassNotFoundException(name);
              }
              byte[] bytes = ByteStreams.toByteArray(in);
              loaded = defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
              throw new ClassNotFoundException(name, e);
            }
          } else {
            loaded = super.loadClass(name, false);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase().startsWith("windows");
  }
}
