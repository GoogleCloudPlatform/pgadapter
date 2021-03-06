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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.io.File;
import java.io.IOException;
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
}
