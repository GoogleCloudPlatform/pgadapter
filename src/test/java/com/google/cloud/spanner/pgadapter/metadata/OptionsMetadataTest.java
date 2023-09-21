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

package com.google.cloud.spanner.pgadapter.metadata;

import static com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.parseSslMode;
import static com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.toServerVersionNum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.TextFormat;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OptionsMetadataTest {

  @Test
  public void testDefaultDomainSocketFile() {
    for (String os : new String[] {"ubuntu", "windows"}) {
      OptionsMetadata options =
          new OptionsMetadata(os, new String[] {"-p", "p", "-i", "i", "-c", "credentials.json"});
      if (options.isWindows()) {
        assertEquals("", options.getSocketFile(5432));
        assertFalse(options.isDomainSocketEnabled());
      } else {
        assertEquals("/tmp" + File.separator + ".s.PGSQL.5432", options.getSocketFile(5432));
        assertTrue(options.isDomainSocketEnabled());
      }
    }
  }

  @Test
  public void testCustomDomainSocketFile() {
    for (String os : new String[] {"ubuntu", "windows"}) {
      OptionsMetadata options =
          new OptionsMetadata(os, new String[] {"-p p", "-i i", "-c \"\"", "-dir /pgadapter"});
      assertEquals(
          "/pgadapter" + File.separatorChar + ".s.PGSQL.5432", options.getSocketFile(5432));
      assertTrue(options.isDomainSocketEnabled());
    }
  }

  @Test
  public void testDefaultMaxBacklog() {
    OptionsMetadata options =
        new OptionsMetadata(new String[] {"-p", "p", "-i", "i", "-c", "credentials.json"});
    assertEquals(1000, options.getMaxBacklog());
  }

  @Test
  public void testCustomMaxBacklog() {
    OptionsMetadata options =
        new OptionsMetadata(
            new String[] {"-p", "p", "-i", "i", "-max_backlog", "100", "-c", "credentials.json"});
    assertEquals(100, options.getMaxBacklog());
  }

  @Test
  public void testDatabaseName() {
    assertFalse(
        new OptionsMetadata(new String[] {"-c", "credentials.json"}).hasDefaultConnectionUrl());
    assertFalse(
        new OptionsMetadata(new String[] {"-p", "p", "-c", "credentials.json"})
            .hasDefaultConnectionUrl());
    assertFalse(
        new OptionsMetadata(new String[] {"-i", "i", "-c", "credentials.json"})
            .hasDefaultConnectionUrl());
    assertFalse(
        new OptionsMetadata(new String[] {"-p", "p", "-i", "i", "-c", "credentials.json"})
            .hasDefaultConnectionUrl());
    assertTrue(
        new OptionsMetadata(
                new String[] {"-p", "p", "-i", "i", "-d", "d", "-c", "credentials.json"})
            .hasDefaultConnectionUrl());
    assertThrows(
        SpannerException.class,
        () -> new OptionsMetadata(new String[] {"-d", "d", "-c", "credentials.json"}));
    assertThrows(
        SpannerException.class,
        () -> new OptionsMetadata(new String[] {"-i", "i", "-d", "d", "-c", "credentials.json"}));
  }

  @Test
  public void testBuildConnectionUrlWithFullPath() {
    assertEquals(
        "cloudspanner:/projects/test-project/instances/test-instance/databases/test-database;userAgent=pg-adapter;credentials=credentials.json",
        new OptionsMetadata(new String[] {"-c", "credentials.json"})
            .buildConnectionURL(
                "projects/test-project/instances/test-instance/databases/test-database"));
    assertEquals(
        "cloudspanner:/projects/test-project/instances/test-instance/databases/test-database;userAgent=pg-adapter;credentials=credentials.json",
        new OptionsMetadata(
                new String[] {
                  "-p", "test-project", "-i", "test-instance", "-c", "credentials.json"
                })
            .buildConnectionURL("test-database"));
  }

  @Test
  public void testMissingProjectId() {
    SpannerException spannerException =
        assertThrows(
            SpannerException.class,
            () -> new OptionsMetadata(new String[] {"-i", "my-instance", "-d", "my-db"}));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testMissingInstanceId() {
    SpannerException spannerException =
        assertThrows(
            SpannerException.class,
            () -> new OptionsMetadata(new String[] {"-p", "my-project", "-d", "my-db"}));
    assertEquals(ErrorCode.INVALID_ARGUMENT, spannerException.getErrorCode());
  }

  @Test
  public void testBuildConnectionUrlWithDefaultProjectId() {
    OptionsMetadata useDefaultProjectIdOptions =
        new OptionsMetadata(new String[] {"-i", "test-instance", "-c", "credentials.json"}) {
          @Override
          String getDefaultProjectId() {
            return "custom-test-project";
          }
        };
    assertEquals(
        "cloudspanner:/projects/custom-test-project/instances/test-instance/databases/test-database;userAgent=pg-adapter;credentials=credentials.json",
        useDefaultProjectIdOptions.buildConnectionURL("test-database"));
    OptionsMetadata noProjectIdOptions =
        new OptionsMetadata(new String[] {"-i", "test-instance", "-c", "credentials.json"}) {
          @Override
          String getDefaultProjectId() {
            return null;
          }
        };
    SpannerException spannerException =
        assertThrows(
            SpannerException.class, () -> noProjectIdOptions.buildConnectionURL("test-database"));
    assertEquals(ErrorCode.FAILED_PRECONDITION, spannerException.getErrorCode());
  }

  @Test
  public void testBuildConnectionUrlWithDefaultCredentials() {
    OptionsMetadata useDefaultCredentials =
        new OptionsMetadata(new String[] {"-p", "test-project", "-i", "test-instance"}) {
          @Override
          void tryGetDefaultCredentials() {}
        };
    assertEquals(
        "cloudspanner:/projects/test-project/instances/test-instance/databases/test-database;userAgent=pg-adapter",
        useDefaultCredentials.buildConnectionURL("test-database"));
    OptionsMetadata noDefaultCredentialsOptions =
        new OptionsMetadata(new String[] {"-p", "test-project", "-i", "test-instance"}) {
          @Override
          void tryGetDefaultCredentials() throws IOException {
            throw new IOException("test exception");
          }
        };
    SpannerException spannerException =
        assertThrows(
            SpannerException.class,
            () -> noDefaultCredentialsOptions.buildConnectionURL("test-database"));
    assertEquals(ErrorCode.FAILED_PRECONDITION, spannerException.getErrorCode());
  }

  @Test
  public void testAuthenticationAndCredentialsNotAllowed() {
    SpannerException exception =
        assertThrows(
            SpannerException.class,
            () -> new OptionsMetadata(new String[] {"-c", "credentials.json", "-a"}));
    assertEquals(ErrorCode.INVALID_ARGUMENT, exception.getErrorCode());
  }

  @Test
  public void testShouldAuthenticate() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-a"});
    assertTrue(options.shouldAuthenticate());
  }

  @Test
  public void testCredentials() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-c", "credentials.json"});
    assertFalse(options.shouldAuthenticate());
    assertEquals("credentials.json", options.buildCredentialsFile());
  }

  @Test
  public void testDisableAutoDetectClient() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    assertTrue(options.shouldAutoDetectClient());

    options = new OptionsMetadata(new String[] {"-p p", "-i i", "-disable_auto_detect_client"});
    assertFalse(options.shouldAutoDetectClient());
  }

  @Test
  public void testDeprecatedBinaryFormat() {
    PrintStream originalOut = System.out;
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      System.setOut(new PrintStream(outputStream));
      OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i", "-b"});
      assertTrue(options.isBinaryFormat());

      assertEquals(
          "Forcing the server to return results using the binary format is a violation "
              + "of the PostgreSQL wire-protocol. Using this option can cause unexpected errors.\nIt is "
              + "recommended not to use the -b option."
              + System.lineSeparator(),
          outputStream.toString());
    } finally {
      System.setOut(originalOut);
    }
  }

  @Test
  public void testDisablePgCatalogReplacements() {
    OptionsMetadata options = new OptionsMetadata(new String[] {"-p p", "-i i"});
    assertTrue(options.replacePgCatalogTables());

    options =
        new OptionsMetadata(new String[] {"-p p", "-i i", "-disable_pg_catalog_replacements"});
    assertFalse(options.replacePgCatalogTables());
  }

  @Test
  public void testToServerVersionNum() {
    assertEquals("10000", toServerVersionNum("1.0"));
    assertEquals("140001", toServerVersionNum("14.1"));
    assertEquals("80004", toServerVersionNum("8.4"));
    assertEquals("10000", toServerVersionNum("1.0.1"));
    assertEquals("10000", toServerVersionNum("1.0 custom build"));
    assertEquals("10010", toServerVersionNum("1.10 custom build"));
  }

  @Test
  public void testParseSslMode() {
    assertEquals(SslMode.Disable, parseSslMode(null));
    assertEquals(SslMode.Disable, parseSslMode("Disable"));
    assertEquals(SslMode.Disable, parseSslMode("disable"));
    assertEquals(SslMode.Disable, parseSslMode("DISABLE"));
    assertEquals(SslMode.Enable, parseSslMode("Enable"));
    assertEquals(SslMode.Enable, parseSslMode("enable"));
    assertEquals(SslMode.Enable, parseSslMode("ENABLE"));
    assertEquals(SslMode.Require, parseSslMode("Require"));
    assertEquals(SslMode.Require, parseSslMode("require"));
    assertEquals(SslMode.Require, parseSslMode("REQUIRE"));
    assertThrows(IllegalArgumentException.class, () -> parseSslMode("foo"));
  }

  @Test
  public void testSslEnabled() {
    assertFalse(SslMode.Disable.isSslEnabled());
    assertTrue(SslMode.Enable.isSslEnabled());
    assertTrue(SslMode.Require.isSslEnabled());
  }

  @Test
  public void testBuilder() {
    assertFalse(
        OptionsMetadata.newBuilder()
            .setProject("my-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .hasDefaultInstanceId());
    assertEquals(
        InstanceId.of("my-project", "my-instance"),
        OptionsMetadata.newBuilder()
            .setProject("my-project")
            .setInstance("my-instance")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getDefaultInstanceId());
    assertEquals(
        DatabaseId.of("my-project", "my-instance", "my-database"),
        OptionsMetadata.newBuilder()
            .setProject("my-project")
            .setInstance("my-instance")
            .setDatabase("my-database")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getDefaultDatabaseId());
    assertEquals(
        "/path/to/credentials.json",
        OptionsMetadata.newBuilder()
            .setCredentialsFile("/path/to/credentials.json")
            .build()
            .buildCredentialsFile());
    assertNull(OptionsMetadata.newBuilder().build().getCredentials());
    assertEquals(
        NoCredentials.getInstance(),
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getCredentials());
    assertNull(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSessionPoolOptions());
    assertEquals(
        SessionPoolOptions.newBuilder().setMinSessions(500).setMaxSessions(1000).build(),
        OptionsMetadata.newBuilder()
            .setSessionPoolOptions(
                SessionPoolOptions.newBuilder().setMinSessions(500).setMaxSessions(1000).build())
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSessionPoolOptions());
    assertNull(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("numChannels"));
    assertEquals(
        "4",
        OptionsMetadata.newBuilder()
            .setNumChannels(4)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("numChannels"));
    assertEquals(
        "16",
        OptionsMetadata.newBuilder()
            .setNumChannels(16)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("numChannels"));
    assertNull(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("databaseRole"));
    assertEquals(
        "my-role",
        OptionsMetadata.newBuilder()
            .setDatabaseRole("my-role")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("databaseRole"));
    assertNull(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("usePlainText"));
    assertEquals(
        "true",
        OptionsMetadata.newBuilder()
            .setUsePlainText()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getPropertyMap()
            .get("usePlainText"));
    assertEquals(
        DdlTransactionMode.Batch,
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getDdlTransactionMode());
    assertEquals(
        DdlTransactionMode.AutocommitImplicitTransaction,
        OptionsMetadata.newBuilder()
            .setDdlTransactionMode(DdlTransactionMode.AutocommitImplicitTransaction)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getDdlTransactionMode());
    assertFalse(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .shouldAuthenticate());
    assertTrue(
        OptionsMetadata.newBuilder()
            .setRequireAuthentication()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .shouldAuthenticate());
    assertFalse(
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .disableLocalhostCheck());
    assertTrue(
        OptionsMetadata.newBuilder()
            .setDisableLocalhostCheck()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .disableLocalhostCheck());
    assertEquals(
        SslMode.Disable,
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSslMode());
    assertEquals(
        SslMode.Require,
        OptionsMetadata.newBuilder()
            .setSslMode(SslMode.Require)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSslMode());
    assertEquals(
        0,
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getProxyPort());
    assertEquals(
        9999,
        OptionsMetadata.newBuilder()
            .setPort(9999)
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getProxyPort());
    boolean isWindows =
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .isWindows();
    assertEquals(
        isWindows ? "" : "/tmp/.s.PGSQL.9999",
        OptionsMetadata.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSocketFile(9999));
    if (!isWindows) {
      assertEquals(
          "/var/pg/.s.PGSQL.5432",
          OptionsMetadata.newBuilder()
              .setUnixDomainSocketDirectory("/var/pg")
              .setCredentials(NoCredentials.getInstance())
              .build()
              .getSocketFile(5432));
    }
    assertEquals(
        "",
        OptionsMetadata.newBuilder()
            .disableUnixDomainSockets()
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getSocketFile(5432));

    assertThrows(
        SpannerException.class,
        () -> OptionsMetadata.newBuilder().setInstance("my-instance").build());
    assertThrows(
        SpannerException.class,
        () -> OptionsMetadata.newBuilder().setDatabase("my-database").build());
    assertThrows(
        SpannerException.class,
        () ->
            OptionsMetadata.newBuilder()
                .setCredentialsFile("/path/to/credentials.json")
                .setRequireAuthentication()
                .build());
    assertThrows(
        SpannerException.class,
        () ->
            OptionsMetadata.newBuilder()
                .setCredentials(OAuth2Credentials.create(AccessToken.newBuilder().build()))
                .setRequireAuthentication()
                .build());
  }

  @Test
  public void testStripJdbcPrefix() {
    assertEquals(
        "cloudspanner:/projects/my-project/instances/my-instance/databases/my-database",
        new OptionsMetadata(
                "linux",
                "jdbc:cloudspanner:/projects/my-project/instances/my-instance/databases/my-database",
                5432,
                TextFormat.POSTGRESQL,
                false,
                false,
                false,
                false,
                null)
            .getDefaultConnectionUrl());
  }
}
