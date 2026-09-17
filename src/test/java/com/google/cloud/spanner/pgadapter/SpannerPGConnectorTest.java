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

import static com.google.cloud.spanner.pgadapter.SpannerPGConnector.configureEnvironment;
import static com.google.cloud.spanner.pgadapter.SpannerPGConnector.createOptionsMetadata;
import static com.google.cloud.spanner.pgadapter.SpannerPGConnector.findConnectionOverride;
import static com.google.cloud.spanner.pgadapter.SpannerPGConnector.getDatabase;
import static com.google.cloud.spanner.pgadapter.SpannerPGConnector.getDatabaseFromArguments;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.SslMode;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the argument and environment handling of {@link SpannerPGConnector}. */
@RunWith(JUnit4.class)
public class SpannerPGConnectorTest {

  private static final Map<String, String> EMPTY_ENVIRONMENT = ImmutableMap.of();

  static final class CapturingOutput {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final PrintStream outStream = new PrintStream(out, true);
    final PrintStream errStream = new PrintStream(err, true);

    String out() {
      outStream.flush();
      return out.toString();
    }

    String err() {
      errStream.flush();
      return err.toString();
    }
  }

  private static int run(CapturingOutput output, String... args) {
    return SpannerPGConnector.run(args, EMPTY_ENVIRONMENT, output.outStream, output.errStream);
  }

  // ---------------------------------------------------------------------------------------------
  // Usage, help and version.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNoArgumentsPrintsUsageToStderr() {
    CapturingOutput output = new CapturingOutput();

    assertEquals(1, run(output));
    assertTrue(output.err(), output.err().contains("Usage: spanner-pg-connector <command>"));
    assertEquals("", output.out());
  }

  @Test
  public void testHelpPrintsUsageToStdout() {
    for (String helpArgument : new String[] {"--help", "-help", "help", "-?"}) {
      CapturingOutput output = new CapturingOutput();

      assertEquals(helpArgument, 0, run(output, helpArgument));
      assertTrue(output.out(), output.out().contains("Usage: spanner-pg-connector <command>"));
      // The usage message should document the environment variables that configure the connection.
      assertTrue(output.out(), output.out().contains("GOOGLE_CLOUD_PROJECT"));
      assertTrue(output.out(), output.out().contains("SPANNER_INSTANCE"));
      assertTrue(output.out(), output.out().contains("SPANNER_EMULATOR_HOST"));
      assertEquals("", output.err());
    }
  }

  @Test
  public void testVersion() {
    for (String versionArgument : new String[] {"--version", "-version", "version", "-V"}) {
      CapturingOutput output = new CapturingOutput();

      assertEquals(versionArgument, 0, run(output, versionArgument));
      assertTrue(output.out(), output.out().startsWith("spanner-pg-connector "));
      assertEquals("", output.err());
    }
  }

  @Test
  public void testUnknownCommandReturnsCommandNotFound() {
    CapturingOutput output = new CapturingOutput();

    assertEquals(
        SpannerPGConnector.EXIT_CODE_COMMAND_NOT_FOUND,
        run(output, "this-command-does-not-exist-1234"));
    assertTrue(output.err(), output.err().contains("this-command-does-not-exist-1234"));
    // The error message must be a readable message and not a Java stack trace.
    assertFalse(output.err(), output.err().contains("\tat com.google.cloud.spanner"));
  }

  // ---------------------------------------------------------------------------------------------
  // Host and port override detection.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConnectionOverrideIsRejected() {
    // The first element of each row is the argument that is expected to be rejected. The rest of
    // the row is the command line.
    String[][] rejectedArguments =
        new String[][] {
          {"-h", "psql", "-h", "localhost"},
          {"-hlocalhost", "psql", "-hlocalhost"},
          {"-p", "psql", "-p", "5432"},
          {"-p5432", "psql", "-p5432"},
          {"--host", "psql", "--host", "localhost"},
          {"--host=localhost", "psql", "--host=localhost"},
          {"--hostaddr=127.0.0.1", "psql", "--hostaddr=127.0.0.1"},
          {"--port", "psql", "--port", "5432"},
          {"--port=5432", "psql", "--port=5432"},
          {"-h", "psql", "-d", "my-database", "-c", "select 1", "-h", "localhost"},
        };
    for (String[] testCase : rejectedArguments) {
      String expected = testCase[0];
      String[] args = Arrays.copyOfRange(testCase, 1, testCase.length);

      assertEquals(String.join(" ", args), expected, findConnectionOverride(args));

      CapturingOutput output = new CapturingOutput();
      assertEquals(String.join(" ", args), 1, run(output, args));
      assertTrue(output.err(), output.err().contains("are not supported"));
    }
  }

  @Test
  public void testArgumentsWithoutConnectionOverride() {
    String[][] acceptedArguments =
        new String[][] {
          {"psql"},
          {"psql", "-d", "my-database"},
          {"psql", "-d", "my-database", "-c", "select 1"},
          {"psql", "-c", "select 1", "-t", "-A"},
          // Capital -P is the psql 'pset' option and must not be mistaken for a port.
          {"psql", "-P", "pager=off"},
          {"pg_dump", "--schema-only", "--no-owner"},
          // Everything after '--' is a positional argument.
          {"psql", "--", "-h", "localhost"},
        };
    for (String[] args : acceptedArguments) {
      assertNull(String.join(" ", args), findConnectionOverride(args));
    }
  }

  @Test
  public void testCommandNameIsNeverTreatedAsAnOption() {
    // The first argument is the command that should be executed and must never be inspected.
    assertNull(findConnectionOverride(new String[] {"-h"}));
    assertNull(getDatabaseFromArguments(new String[] {"-d"}));
  }

  // ---------------------------------------------------------------------------------------------
  // Database detection.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testGetDatabaseFromArguments() {
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "-d", "my-db"}));
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "-dmy-db"}));
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "--dbname", "my-db"}));
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "--dbname=my-db"}));
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "--database", "my-db"}));
    assertEquals("my-db", getDatabaseFromArguments(new String[] {"psql", "--database=my-db"}));
    assertEquals(
        "my-db", getDatabaseFromArguments(new String[] {"psql", "-c", "select 1", "-d", "my-db"}));
    assertEquals(
        "projects/p/instances/i/databases/d",
        getDatabaseFromArguments(
            new String[] {"psql", "-d", "projects/p/instances/i/databases/d", "-c", "select 1"}));

    assertNull(getDatabaseFromArguments(new String[] {"psql"}));
    assertNull(getDatabaseFromArguments(new String[] {"psql", "-c", "select 1"}));
    // -d without a value is invalid, but must not throw an ArrayIndexOutOfBoundsException.
    assertNull(getDatabaseFromArguments(new String[] {"psql", "-d"}));
    // Everything after '--' is a positional argument.
    assertNull(getDatabaseFromArguments(new String[] {"psql", "--", "-d", "my-db"}));
  }

  @Test
  public void testGetDatabaseFallsBackToEnvironment() {
    Map<String, String> environment = ImmutableMap.of("SPANNER_DATABASE", "db-from-environment");

    assertEquals("db-from-environment", getDatabase(new String[] {"psql"}, environment));
    // An explicit -d argument takes precedence over the environment variable.
    assertEquals(
        "db-from-argument",
        getDatabase(new String[] {"psql", "-d", "db-from-argument"}, environment));
    assertNull(getDatabase(new String[] {"psql"}, EMPTY_ENVIRONMENT));
    assertNull(getDatabase(new String[] {"psql"}, ImmutableMap.of("SPANNER_DATABASE", "")));
  }

  // ---------------------------------------------------------------------------------------------
  // Client environment.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConfigureEnvironment() {
    Map<String, String> environment = new HashMap<>();
    configureEnvironment(environment, 12345, "my-db");

    assertEquals("localhost", environment.get("PGHOST"));
    assertEquals("12345", environment.get("PGPORT"));
    assertEquals("my-db", environment.get("PGDATABASE"));
    assertEquals("disable", environment.get("PGSSLMODE"));
    assertEquals("disable", environment.get("PGGSSENCMODE"));
  }

  @Test
  public void testConfigureEnvironmentWithoutDatabase() {
    Map<String, String> environment = new HashMap<>();
    configureEnvironment(environment, 12345, null);

    assertFalse(environment.containsKey("PGDATABASE"));
  }

  @Test
  public void testConfigureEnvironmentRemovesConnectionOverrides() {
    Map<String, String> environment = new HashMap<>();
    // These would all redirect the client tool away from PGAdapter, or would make it require a
    // connection feature that PGAdapter does not support when it is started by the connector.
    environment.put("PGHOSTADDR", "127.0.0.2");
    environment.put("PGSERVICE", "my-service");
    environment.put("PGSERVICEFILE", "/tmp/pg_service.conf");
    environment.put("PGREQUIRESSL", "1");
    environment.put("PGCHANNELBINDING", "require");
    environment.put("PGSSLNEGOTIATION", "direct");
    environment.put("PGSSLMODE", "require");
    // Variables that are not connection overrides must be left alone.
    environment.put("PGCLIENTENCODING", "UTF8");
    environment.put("PGAPPNAME", "my-app");

    configureEnvironment(environment, 12345, null);

    assertFalse(environment.containsKey("PGHOSTADDR"));
    assertFalse(environment.containsKey("PGSERVICE"));
    assertFalse(environment.containsKey("PGSERVICEFILE"));
    assertFalse(environment.containsKey("PGREQUIRESSL"));
    assertFalse(environment.containsKey("PGCHANNELBINDING"));
    assertFalse(environment.containsKey("PGSSLNEGOTIATION"));
    assertEquals("disable", environment.get("PGSSLMODE"));
    assertEquals("UTF8", environment.get("PGCLIENTENCODING"));
    assertEquals("my-app", environment.get("PGAPPNAME"));
  }

  // ---------------------------------------------------------------------------------------------
  // OptionsMetadata.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testCreateOptionsMetadata() {
    OptionsMetadata options =
        createOptionsMetadata(
            ImmutableMap.of(
                "GOOGLE_CLOUD_PROJECT", "my-project", "SPANNER_INSTANCE", "my-instance"));

    // Port 0 means that the proxy binds to a dynamically assigned port.
    assertEquals(0, options.getProxyPort());
    assertEquals(SslMode.Disable, options.getSslMode());
    assertFalse(options.isDomainSocketEnabled());
    assertTrue(options.hasDefaultInstanceId());
  }

  @Test
  public void testDdlTransactionModeAllowsDdlInTransactionsAndMixedBatches() {
    // The default (Batch) rejects DDL in explicit transactions and in mixed batches.
    assertEquals(
        DdlTransactionMode.AutocommitExplicitTransaction,
        createOptionsMetadata(EMPTY_ENVIRONMENT).getDdlTransactionMode());
  }

  @Test
  public void testCreateOptionsMetadataWithoutEnvironment() {
    OptionsMetadata options = createOptionsMetadata(EMPTY_ENVIRONMENT);

    assertEquals(0, options.getProxyPort());
    assertEquals(SslMode.Disable, options.getSslMode());
    assertFalse(options.isDomainSocketEnabled());
    assertFalse(options.hasDefaultInstanceId());
  }

  @Test
  public void testCreateOptionsMetadataForEmulator() {
    OptionsMetadata options =
        createOptionsMetadata(
            ImmutableMap.of(
                "GOOGLE_CLOUD_PROJECT",
                "my-project",
                "SPANNER_INSTANCE",
                "my-instance",
                "SPANNER_EMULATOR_HOST",
                "localhost:9010"));

    assertEquals("true", options.getPropertyMap().get("autoConfigEmulator"));
  }

  @Test
  public void testEmptyEnvironmentVariablesAreIgnored() {
    // An empty environment variable must be treated the same as an unset environment variable.
    OptionsMetadata options =
        createOptionsMetadata(
            ImmutableMap.of(
                "GOOGLE_CLOUD_PROJECT", "", "SPANNER_INSTANCE", "", "SPANNER_EMULATOR_HOST", ""));

    assertFalse(options.hasDefaultInstanceId());
  }
}
