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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verifies that container startup scripts, Dockerfiles, and {@code spanner-pg-connector} launchers
 * pass {@code -XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2} in multi-arch-safe order while
 * preserving shell word-splitting invariants relied upon by Cloud Run and GKE manifests.
 */
@RunWith(JUnit4.class)
public class StartupScriptsJvmOptionsTest {

  private static final String IGNORE_UNRECOGNIZED_FLAG = "-XX:+IgnoreUnrecognizedVMOptions";
  private static final String USE_AVX2_FLAG = "-XX:UseAVX=2";
  private static final String EXPECTED_ENV_LINE =
      "ENV JDK_JAVA_OPTIONS=\"-XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2\"";
  private static final String EXPECTED_DISTROLESS_ENTRYPOINT =
      "ENTRYPOINT [\"/usr/bin/java\", \"-XX:+IgnoreUnrecognizedVMOptions\", \"-XX:UseAVX=2\","
          + " \"-jar\", \"pgadapter.jar\"]";

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Creates an executable stub {@code java} script that records every received CLI argument (one
   * per line) to {@code capturedArgsFile} and exits cleanly.
   */
  private Path createRecordingJavaStub(Path binDir, Path capturedArgsFile) throws IOException {
    Files.createDirectories(binDir);
    Path javaStub = binDir.resolve("java");
    String script =
        "#!/bin/sh\n"
            + ": > \""
            + capturedArgsFile.toAbsolutePath()
            + "\"\n"
            + "for arg in \"$@\"; do\n"
            + "  printf '%s\\n' \"$arg\" >> \""
            + capturedArgsFile.toAbsolutePath()
            + "\"\n"
            + "done\n";
    Files.write(javaStub, script.getBytes(StandardCharsets.UTF_8));
    assertTrue("Failed to mark stub java executable", javaStub.toFile().setExecutable(true));
    return javaStub;
  }

  /**
   * Runs a launcher command with {@code stubBinDir} prepended to {@code PATH} and returns the exact
   * argument list received by the stub {@code java} binary.
   */
  private List<String> runLauncherWithJavaStub(
      Path stubBinDir, Path capturedArgsFile, String... cmd)
      throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(cmd);
    String currentPath = pb.environment().getOrDefault("PATH", "/usr/bin:/bin");
    pb.environment().put("PATH", stubBinDir.toAbsolutePath() + File.pathSeparator + currentPath);
    Process process = pb.start();
    try {
      assertTrue(
          "Launcher timed out: " + Arrays.toString(cmd), process.waitFor(15, TimeUnit.SECONDS));
      assertEquals("Launcher exited with non-zero code", 0, process.exitValue());
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
    return Files.readAllLines(capturedArgsFile, StandardCharsets.UTF_8);
  }

  @Test
  public void testBuildStartupScriptPassesUseAvx2AndPreservesWordSplitting() throws Exception {
    assumeFalse(
        "Requires POSIX shell", System.getProperty("os.name", "").toLowerCase().contains("win"));

    Path binDir = tempFolder.newFolder("bin").toPath();
    Path captured = tempFolder.newFile("startup_args.txt").toPath();
    createRecordingJavaStub(binDir, captured);

    // Cloud Run and GKE sidecar manifests pass combined "-dir /sockets" strings as single argv
    // elements and rely on startup.sh word-splitting them before exec.
    List<String> args =
        runLauncherWithJavaStub(
            binDir,
            captured,
            "/bin/bash",
            Paths.get("build", "startup.sh").toAbsolutePath().toString(),
            "-Djavax.net.ssl.keyStore=/etc/ssl/keystore.jks",
            "-dir /sockets",
            "-x");

    assertEquals(
        Arrays.asList(
            IGNORE_UNRECOGNIZED_FLAG,
            USE_AVX2_FLAG,
            "-Djavax.net.ssl.keyStore=/etc/ssl/keystore.jks",
            "-jar",
            "pgadapter.jar",
            "-dir",
            "/sockets",
            "-x"),
        args);
  }

  @Test
  public void testEmulatorStartupScriptPassesUseAvx2AndPreservesWordSplitting() throws Exception {
    assumeFalse(
        "Requires POSIX shell", System.getProperty("os.name", "").toLowerCase().contains("win"));
    // Avoid starting a real emulator daemon if this test ever runs inside the emulator container.
    assumeFalse(
        "Skipped when /emulator/gateway_main is present on host",
        Files.exists(Paths.get("/emulator/gateway_main")));

    Path binDir = tempFolder.newFolder("emulator_bin").toPath();
    Path captured = tempFolder.newFile("emulator_args.txt").toPath();
    createRecordingJavaStub(binDir, captured);

    List<String> args =
        runLauncherWithJavaStub(
            binDir,
            captured,
            "/bin/bash",
            Paths.get("build", "emulator", "startup.sh").toAbsolutePath().toString(),
            "-Dcustom.property=enabled",
            "-d test-db");

    int ignoreIdx = args.indexOf(IGNORE_UNRECOGNIZED_FLAG);
    int avxIdx = args.indexOf(USE_AVX2_FLAG);
    int sysPropIdx = args.indexOf("-Dcustom.property=enabled");
    int jarIdx = args.indexOf("-jar");
    assertEquals("IgnoreUnrecognizedVMOptions must be first JVM flag", 0, ignoreIdx);
    assertEquals("UseAVX=2 must immediately follow IgnoreUnrecognizedVMOptions", 1, avxIdx);
    assertTrue("System property must precede -jar", sysPropIdx > avxIdx && sysPropIdx < jarIdx);
    assertEquals("pgadapter.jar", args.get(jarIdx + 1));
    assertEquals("-d", args.get(args.size() - 2));
    assertEquals("test-db", args.get(args.size() - 1));
  }

  @Test
  public void testSpannerPgConnectorUnixLauncherPassesUseAvx2InOrder() throws Exception {
    assumeFalse(
        "Requires POSIX shell", System.getProperty("os.name", "").toLowerCase().contains("win"));

    Path bundleDir = tempFolder.newFolder("spgc").toPath();
    Path launcherCopy = bundleDir.resolve("spanner-pg-connector");
    Files.copy(
        Paths.get("spanner-pg-connector", "spanner-pg-connector"),
        launcherCopy,
        StandardCopyOption.REPLACE_EXISTING);
    assertTrue(launcherCopy.toFile().setExecutable(true));

    Path customJreBin = bundleDir.resolve("custom-jre").resolve("bin");
    Path captured = tempFolder.newFile("spgc_args.txt").toPath();
    createRecordingJavaStub(customJreBin, captured);

    List<String> args =
        runLauncherWithJavaStub(
            customJreBin, captured, "/bin/sh", launcherCopy.toString(), "psql", "-d", "my-db");

    assertEquals(IGNORE_UNRECOGNIZED_FLAG, args.get(0));
    assertEquals(USE_AVX2_FLAG, args.get(1));
    int mainClassIdx = args.indexOf("com.google.cloud.spanner.pgadapter.SpannerPGConnector");
    assertTrue("Main class must follow JVM options", mainClassIdx > 1);
    assertEquals(Arrays.asList("psql", "-d", "my-db"), args.subList(mainClassIdx + 1, args.size()));
  }

  @Test
  public void testSpannerPgConnectorWindowsLauncherPassesUseAvx2InOrder() throws IOException {
    String cmdContent =
        new String(
            Files.readAllBytes(Paths.get("spanner-pg-connector", "spanner-pg-connector.cmd")),
            StandardCharsets.UTF_8);
    assertTrue(
        "Windows spanner-pg-connector.cmd must pass IgnoreUnrecognizedVMOptions before UseAVX=2",
        cmdContent.contains(
            "%JAVA_CMD% -XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2"
                + " --enable-native-access=ALL-UNNAMED"));
  }

  @Test
  public void testDockerfilesConfigureUseAvx2DefenseInDepth() throws IOException {
    List<Path> dockerfiles =
        Arrays.asList(
            Paths.get("build", "Dockerfile"),
            Paths.get("build", "Dockerfile_moss"),
            Paths.get("build", "distroless", "Dockerfile"),
            Paths.get("build", "distroless", "Dockerfile_moss"),
            Paths.get("build", "emulator", "Dockerfile"));

    for (Path dockerfile : dockerfiles) {
      String content = new String(Files.readAllBytes(dockerfile), StandardCharsets.UTF_8);
      int lastFromIdx = content.lastIndexOf("FROM ");
      assertTrue("Missing FROM in " + dockerfile, lastFromIdx >= 0);
      String releaseStage = content.substring(lastFromIdx);
      assertTrue(
          dockerfile + " release stage must set " + EXPECTED_ENV_LINE,
          releaseStage.contains(EXPECTED_ENV_LINE));
    }

    for (Path distrolessDockerfile :
        Arrays.asList(
            Paths.get("build", "distroless", "Dockerfile"),
            Paths.get("build", "distroless", "Dockerfile_moss"))) {
      String content = new String(Files.readAllBytes(distrolessDockerfile), StandardCharsets.UTF_8);
      assertTrue(
          distrolessDockerfile + " must set " + EXPECTED_DISTROLESS_ENTRYPOINT,
          content.contains(EXPECTED_DISTROLESS_ENTRYPOINT));
    }
  }

  @Test
  public void testSampleAndBenchmarkJavaLaunchersPassUseAvx2() throws IOException {
    String cloudRunDockerfile =
        new String(
            Files.readAllBytes(Paths.get("samples", "java", "cloud-run", "Dockerfile")),
            StandardCharsets.UTF_8);
    assertTrue(
        "samples/java/cloud-run/Dockerfile CMD must pass IgnoreUnrecognizedVMOptions before UseAVX=2",
        cloudRunDockerfile.contains(
            "CMD [\"java\", \"-XX:+IgnoreUnrecognizedVMOptions\", \"-XX:UseAVX=2\","));

    for (Path benchmarkScript :
        Arrays.asList(
            Paths.get("benchmarks", "latency-comparison", "golang", "startup.sh"),
            Paths.get("benchmarks", "latency-comparison", "nodejs", "startup.sh"),
            Paths.get("benchmarks", "ycsb", "run.sh"))) {
      String content = new String(Files.readAllBytes(benchmarkScript), StandardCharsets.UTF_8);
      assertTrue(
          benchmarkScript + " must pass -XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2 to java",
          content.contains("java -XX:+IgnoreUnrecognizedVMOptions -XX:UseAVX=2 "));
    }
  }
}
