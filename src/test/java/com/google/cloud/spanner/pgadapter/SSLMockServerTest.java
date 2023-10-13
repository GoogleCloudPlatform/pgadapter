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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SSLMockServerTest extends AbstractMockServerTest {
  private static String originalKeystore;
  private static String originalKeystorePwd;
  private static String publicKeyFile;

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    assumeTrue("This test requires keytool to be installed", isKeytoolAvailable());

    String keystore = generateSSLKey();
    publicKeyFile = exportPublicKey(keystore);
    originalKeystore = System.getProperty("javax.net.ssl.keyStore");
    originalKeystorePwd = System.getProperty("javax.net.ssl.keyStorePassword");
    System.setProperty("javax.net.ssl.keyStore", keystore);
    System.setProperty("javax.net.ssl.keyStorePassword", "password");

    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(), "d", builder -> builder.setSslMode(SslMode.Require));
  }

  @AfterClass
  public static void resetKeystore() {
    if (originalKeystore != null) {
      System.setProperty("javax.net.ssl.keyStore", originalKeystore);
    }
    if (originalKeystorePwd != null) {
      System.setProperty("javax.net.ssl.keyStorePassword", originalKeystorePwd);
    }
  }

  private static String generateSSLKey() throws Exception {
    File keystore = File.createTempFile("serverkeystore", ".p12");
    // Delete the temp file as keytool does not allow overwriting an existing file.
    assertTrue(keystore.delete());
    ProcessBuilder keytoolBuilder = new ProcessBuilder();
    String[] keytoolCommand =
        new String[] {
          "keytool",
          "-genkey",
          "-storetype",
          "PKCS12",
          "-keypass",
          "password",
          "-storepass",
          "password",
          "-keystore",
          keystore.getAbsolutePath(),
          "-keyalg",
          "RSA",
          "-dname",
          String.format(
              "CN=%s,OU=\"Cloud Spanner\",O=Google,L=Amsterdam,ST=\"North Holland\",C=NL",
              InetAddress.getLocalHost().getHostName()),
          "-ext",
          String.format(
              "SAN=DNS:%s,IP:%s,DNS:localhost",
              InetAddress.getLocalHost().getHostName(), InetAddress.getLocalHost().getHostAddress())
        };
    keytoolBuilder.command(keytoolCommand);
    Process keytoolProcess = keytoolBuilder.start();

    int res = keytoolProcess.waitFor();
    assertEquals(0, res);

    return keystore.getAbsolutePath();
  }

  private static String exportPublicKey(String keystore) throws Exception {
    File keyfile = File.createTempFile("keyfile", ".pem");
    // Delete the temp file as keytool does not allow overwriting an existing file.
    assertTrue(keyfile.delete());
    ProcessBuilder keytoolBuilder = new ProcessBuilder();
    String[] keytoolCommand =
        new String[] {
          "keytool",
          "-exportcert",
          "-keystore",
          keystore,
          "-storepass",
          "password",
          "-rfc",
          "-file",
          keyfile.getAbsolutePath()
        };
    keytoolBuilder.command(keytoolCommand);
    Process keytoolProcess = keytoolBuilder.start();

    int res = keytoolProcess.waitFor();
    assertEquals(0, res);

    return keyfile.getAbsolutePath();
  }

  private static boolean isPsqlAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand = new String[] {"psql", "--version"};
    builder.command(psqlCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  private static boolean isKeytoolAvailable() {
    ProcessBuilder builder = new ProcessBuilder();
    String[] keytoolCommand = new String[] {"keytool"};
    builder.command(keytoolCommand);
    try {
      Process process = builder.start();
      int res = process.waitFor();

      return res == 0;
    } catch (Exception ignored) {
      return false;
    }
  }

  @Test
  public void testDisableSSL() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          String.format("sslmode=disable host=localhost port=%d", pgServer.getLocalPort()),
          "-c",
          "SELECT 1;"
        };
    builder.command(psqlCommand);
    Process process = builder.start();

    String output;
    String errors;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertTrue(errors.contains("FATAL:  This proxy requires SSL."));
    assertEquals("", output);
    int res = process.waitFor();
    assertNotEquals(0, res);

    assertEquals(
        0L, pgServer.getDebugMessages().stream().filter(m -> m instanceof SSLMessage).count());
  }

  @Test
  public void testSSLRequire() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          String.format("sslmode=require host=localhost port=%d", pgServer.getLocalPort()),
          "-c",
          "SELECT 1;"
        };
    builder.command(psqlCommand);
    Process process = builder.start();

    String output;
    String errors;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals(" C \n---\n 1\n(1 row)\n", output);
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(
        1L, pgServer.getDebugMessages().stream().filter(m -> m instanceof SSLMessage).count());
  }

  @Test
  public void testSSLVerifyCA() throws Exception {
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          String.format(
              "sslmode=verify-ca sslrootcert=%s host=localhost port=%d",
              publicKeyFile, pgServer.getLocalPort()),
          "-c",
          "SELECT 1;"
        };
    builder.command(psqlCommand);
    Process process = builder.start();

    String output;
    String errors;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals(" C \n---\n 1\n(1 row)\n", output);
    int res = process.waitFor();
    assertEquals(0, res);

    assertEquals(
        1L, pgServer.getDebugMessages().stream().filter(m -> m instanceof SSLMessage).count());
  }

  @Test
  public void testSSLVerifyFull() throws Exception {
    for (String host :
        new String[] {
          InetAddress.getLocalHost().getHostName(),
          InetAddress.getLocalHost().getHostAddress(),
          "localhost"
        }) {
      ProcessBuilder builder = new ProcessBuilder();
      String[] psqlCommand =
          new String[] {
            "psql",
            String.format(
                "sslmode=verify-full sslrootcert=%s host=%s port=%d",
                publicKeyFile, InetAddress.getLocalHost().getHostName(), pgServer.getLocalPort()),
            "-c",
            "SELECT 1;"
          };
      builder.command(psqlCommand);
      Process process = builder.start();

      String output;
      String errors;

      try (BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
          BufferedReader errorReader =
              new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
        errors = errorReader.lines().collect(Collectors.joining("\n"));
        output = reader.lines().collect(Collectors.joining("\n"));
      }

      // Ignore known errors on some Macs on GitHub Actions.
      if (!(host.startsWith("Mac-")
          && errors.contains("error: could not translate host name \"Mac-"))) {
        assertEquals(host, "", errors);
        assertEquals(" C \n---\n 1\n(1 row)\n", output);
        int res = process.waitFor();
        assertEquals(0, res);

        assertEquals(
            1L, pgServer.getDebugMessages().stream().filter(m -> m instanceof SSLMessage).count());
      }
      pgServer.clearDebugMessages();
    }
  }
}
