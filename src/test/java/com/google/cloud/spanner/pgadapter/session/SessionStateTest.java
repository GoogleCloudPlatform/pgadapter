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

package com.google.cloud.spanner.pgadapter.session;

import static com.google.cloud.spanner.pgadapter.session.SessionState.tryGet;
import static com.google.cloud.spanner.pgadapter.session.SessionState.tryGetFirstNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SessionStateTest {
  @Test
  public void testShowInitialSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());
  }

  @Test
  public void testShowLocalSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());

    state.setLocal(null, "client_encoding", "SQL_ASCII");
    assertEquals("SQL_ASCII", state.get(null, "client_encoding").getSetting());

    // Local settings are not persisted after a commit.
    state.commit();
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());

    // Also verify that a rollback clears any local setting.
    state.setLocal(null, "client_encoding", "SQL_ASCII");
    assertEquals("SQL_ASCII", state.get(null, "client_encoding").getSetting());
    state.rollback();
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());
  }

  @Test
  public void testCommitSessionSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());

    state.set(null, "client_encoding", "SQL_ASCII");
    assertEquals("SQL_ASCII", state.get(null, "client_encoding").getSetting());

    // Session settings are persisted after a commit.
    state.commit();
    assertEquals("SQL_ASCII", state.get(null, "client_encoding").getSetting());
  }

  @Test
  public void testRollbackSessionSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());

    state.set(null, "client_encoding", "SQL_ASCII");
    assertEquals("SQL_ASCII", state.get(null, "client_encoding").getSetting());

    state.rollback();
    assertEquals("UTF8", state.get(null, "client_encoding").getSetting());
  }

  @Test
  public void testCommitSessionSettingHiddenBehindLocalSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertNull(state.get(null, "application_name").getSetting());

    state.set(null, "application_name", "my-app");
    assertEquals("my-app", state.get(null, "application_name").getSetting());

    // This will set the application_name for this transaction only.
    // The session value will (in the background) be 'my-app', which is what will be committed.
    state.setLocal(null, "application_name", "local-app");
    assertEquals("local-app", state.get(null, "application_name").getSetting());

    state.commit();
    assertEquals("my-app", state.get(null, "application_name").getSetting());
  }

  @Test
  public void testRollbackSessionSettingHiddenBehindLocalSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertNull(state.get(null, "application_name").getSetting());

    state.set(null, "application_name", "my-app");
    assertEquals("my-app", state.get(null, "application_name").getSetting());

    // This will set the application_name for this transaction only.
    // The session value will (in the background) be 'my-app', which is what will be committed.
    state.setLocal(null, "application_name", "local-app");
    assertEquals("local-app", state.get(null, "application_name").getSetting());

    state.rollback();
    assertNull(state.get(null, "application_name").getSetting());
  }

  @Test
  public void testOverwriteLocalSettingWithNewLocalSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertNull(state.get(null, "application_name").getSetting());

    state.setLocal(null, "application_name", "local-app1");
    assertEquals("local-app1", state.get(null, "application_name").getSetting());

    state.setLocal(null, "application_name", "local-app2");
    assertEquals("local-app2", state.get(null, "application_name").getSetting());

    state.commit();
    assertNull(state.get(null, "application_name").getSetting());
  }

  @Test
  public void testOverwriteLocalSettingWithSessionSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertNull(state.get(null, "application_name").getSetting());

    state.setLocal(null, "application_name", "local-app");
    assertEquals("local-app", state.get(null, "application_name").getSetting());

    state.set(null, "application_name", "my-app");
    assertEquals("my-app", state.get(null, "application_name").getSetting());

    state.commit();
    assertEquals("my-app", state.get(null, "application_name").getSetting());
  }

  @Test
  public void testGetAll() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    List<PGSetting> allSettings = state.getAll();
    assertEquals(360, allSettings.size());
  }

  @Test
  public void testResetAll() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "application_name", "my-app");
    state.commit();
    assertEquals("my-app", state.get(null, "application_name").getSetting());

    state.resetAll();
    assertNull(state.get(null, "application_name").getSetting());
  }

  @Test
  public void testSetDefault() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "search_path", "my_schema");
    state.commit();
    assertEquals("my_schema", state.get(null, "search_path").getSetting());

    state.set(null, "search_path", null);
    assertEquals("public", state.get(null, "search_path").getSetting());
  }

  @Test
  public void testGetAllWithLocalAndSessionChanges() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "application_name", "my-app");
    state.setLocal(null, "client_encoding", "my-encoding");

    state.set("spanner", "custom_session_setting", "value1");
    state.setLocal("spanner", "custom_local_setting", "value2");

    List<PGSetting> allSettings = state.getAll();
    assertEquals(362, allSettings.size());

    PGSetting applicationName =
        allSettings.stream()
            .filter(pgSetting -> pgSetting.getName().equals("application_name"))
            .findAny()
            .orElse(mock(PGSetting.class));
    assertEquals("my-app", applicationName.getSetting());
    PGSetting clientEncoding =
        allSettings.stream()
            .filter(pgSetting -> pgSetting.getName().equals("client_encoding"))
            .findAny()
            .orElse(mock(PGSetting.class));
    assertEquals("my-encoding", clientEncoding.getSetting());
    PGSetting customSessionSetting =
        allSettings.stream()
            .filter(pgSetting -> pgSetting.getName().equals("custom_session_setting"))
            .findAny()
            .orElse(mock(PGSetting.class));
    assertEquals("value1", customSessionSetting.getSetting());
    PGSetting customLocalSetting =
        allSettings.stream()
            .filter(pgSetting -> pgSetting.getName().equals("custom_local_setting"))
            .findAny()
            .orElse(mock(PGSetting.class));
    assertEquals("value2", customLocalSetting.getSetting());
  }

  @Test
  public void testShowUnknownSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.get(null, "some_random_setting"));
    assertEquals(
        "INVALID_ARGUMENT: unrecognized configuration parameter \"some_random_setting\"",
        exception.getMessage());
  }

  @Test
  public void testShowUnknownExtensionSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(
            SpannerException.class, () -> state.get("some_extension", "some_random_setting"));
    assertEquals(
        "INVALID_ARGUMENT: unrecognized configuration parameter \"some_extension.some_random_setting\"",
        exception.getMessage());
  }

  @Test
  public void testSetUnknownSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "some_random_setting", "value"));
    assertEquals(
        "INVALID_ARGUMENT: unrecognized configuration parameter \"some_random_setting\"",
        exception.getMessage());
  }

  @Test
  public void testSetUnknownExtensionSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    // Setting an unknown extension setting is allowed.
    state.set("some_extension", "some_random_setting", "my value");

    assertEquals("my value", state.get("some_extension", "some_random_setting").getSetting());
  }

  @Test
  public void testSetValidBoolValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "check_function_bodies", "on");
    assertEquals("on", state.get(null, "check_function_bodies").getSetting());

    state.set(null, "check_function_bodies", "off");
    assertEquals("off", state.get(null, "check_function_bodies").getSetting());
  }

  @Test
  public void testSetInvalidBoolValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(
            SpannerException.class, () -> state.set(null, "check_function_bodies", "random_value"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"check_function_bodies\" requires a Boolean value",
        exception.getMessage());
  }

  @Test
  public void testSetValidIntegerValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "effective_cache_size", "10000");
    assertEquals("10000", state.get(null, "effective_cache_size").getSetting());

    state.set(null, "effective_cache_size", "20000");
    assertEquals("20000", state.get(null, "effective_cache_size").getSetting());
  }

  @Test
  public void testSetInvalidIntegerValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(
            SpannerException.class, () -> state.set(null, "effective_cache_size", "random_value"));
    assertEquals(
        "INVALID_ARGUMENT: invalid value for parameter \"effective_cache_size\": \"random_value\"",
        exception.getMessage());
  }

  @Test
  public void testSetValidRealValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "cpu_tuple_cost", "0.02");
    assertEquals("0.02", state.get(null, "cpu_tuple_cost").getSetting());

    state.set(null, "cpu_tuple_cost", "0.01");
    assertEquals("0.01", state.get(null, "cpu_tuple_cost").getSetting());
  }

  @Test
  public void testSetInvalidRealValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(
            SpannerException.class, () -> state.set(null, "cpu_tuple_cost", "random_value"));
    assertEquals(
        "INVALID_ARGUMENT: invalid value for parameter \"cpu_tuple_cost\": \"random_value\"",
        exception.getMessage());
  }

  @Test
  public void testSetValidEnumValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    state.set(null, "bytea_output", "hex");
    assertEquals("hex", state.get(null, "bytea_output").getSetting());

    state.set(null, "bytea_output", "escape");
    assertEquals("escape", state.get(null, "bytea_output").getSetting());
  }

  @Test
  public void testSetInvalidEnumValue() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PGException exception =
        assertThrows(PGException.class, () -> state.set(null, "bytea_output", "random_value"));
    assertEquals(
        "invalid value for parameter \"bytea_output\": \"random_value\"", exception.getMessage());
    assertEquals("Available values: escape, hex.", exception.getHints());
  }

  @Test
  public void testSetInternalParam() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "segment_size", "100"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"segment_size\" cannot be changed", exception.getMessage());
  }

  @Test
  public void testSetPostmasterParam() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "shared_buffers", "100"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"shared_buffers\" cannot be changed without restarting the server",
        exception.getMessage());
  }

  @Test
  public void testSetSighupParam() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "ssl", "off"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"ssl\" cannot be changed now", exception.getMessage());
  }

  @Test
  public void testSetsuperuserBackendParam() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "jit_debugging_support", "off"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"jit_debugging_support\" cannot be set after connection start",
        exception.getMessage());
  }

  @Test
  public void testSetBackendParam() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "post_auth_delay", "100"));
    assertEquals(
        "INVALID_ARGUMENT: parameter \"post_auth_delay\" cannot be set after connection start",
        exception.getMessage());
  }

  static String getDefaultSessionStateExpression() {
    return "pg_settings_inmem_ as (\n"
        + "select t->>'name' as name, t->>'setting' as setting, t->>'unit' as unit, t->>'category' as category, t->>'short_desc' as short_desc, t->>'extra_desc' as extra_desc, t->>'context' as context, t->>'vartype' as vartype, t->>'min_val' as min_val, t->>'max_val' as max_val, case when t->>'enumvals' is null then null::text[] else spanner.string_array((t->>'enumvals')::jsonb) end as enumvals, t->>'boot_val' as boot_val, t->>'reset_val' as reset_val, t->>'source' as source, (t->>'sourcefile')::varchar as sourcefile, (t->>'sourceline')::bigint as sourceline, (t->>'pending_restart')::boolean as pending_restart\n"
        + "from unnest(array[\n"
        + "'{\"name\":\"DateStyle\",\"setting\":\"ISO, MDY\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"ISO, MDY\",\"reset_val\":\"ISO, MDY\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"TimeZone\",\"setting\":\"Europe/Berlin\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"GMT\",\"reset_val\":\"Europe/Berlin\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"application_name\",\"setting\":null,\"unit\":null,\"category\":\"Reporting and Logging / What to Log\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"\",\"reset_val\":null,\"source\":\"client\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"bytea_output\",\"setting\":\"hex\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"escape\", \"hex\"],\"boot_val\":\"hex\",\"reset_val\":\"hex\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"default_transaction_isolation\",\"setting\":\"serializable\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"],\"boot_val\":\"serializable\",\"reset_val\":\"serializable\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"default_transaction_read_only\",\"setting\":\"off\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"extra_float_digits\",\"setting\":\"1\",\"unit\":null,\"category\":\"Client Connection Defaults / Locale and Formatting\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":\"-15\",\"max_val\":\"3\",\"enum_vals\":null,\"boot_val\":\"1\",\"reset_val\":\"1\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"max_connections\",\"setting\":\"100\",\"unit\":null,\"category\":\"Connections and Authentication / Connection Settings\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"postmaster\",\"vartype\":\"integer\",\"min_val\":\"1\",\"max_val\":\"262143\",\"enum_vals\":null,\"boot_val\":\"100\",\"reset_val\":\"100\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"max_index_keys\",\"setting\":\"16\",\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":\"16\",\"max_val\":\"16\",\"enum_vals\":null,\"boot_val\":\"16\",\"reset_val\":\"16\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"port\",\"setting\":\"5432\",\"unit\":null,\"category\":\"Connections and Authentication / Connection Settings\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"postmaster\",\"vartype\":\"integer\",\"min_val\":\"1\",\"max_val\":\"65535\",\"enum_vals\":null,\"boot_val\":\"5432\",\"reset_val\":\"5432\",\"source\":\"configuration file\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"search_path\",\"setting\":\"public\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"public\",\"reset_val\":\"public\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"server_version\",\"setting\":null,\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"backend\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"14.1\",\"reset_val\":\"14.1\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"server_version_num\",\"setting\":null,\"unit\":null,\"category\":\"Preset Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":\"140001\",\"max_val\":\"140001\",\"enum_vals\":null,\"boot_val\":\"140001\",\"reset_val\":\"140001\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_batch_size\",\"setting\":\"5000\",\"unit\":null,\"category\":\"COPY / Batch size for non-atomic COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"5000\",\"reset_val\":\"5000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_priority\",\"setting\":\"medium\",\"unit\":null,\"category\":\"COPY / RPC priority for commits for COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"low\", \"medium\", \"high\"],\"boot_val\":\"medium\",\"reset_val\":\"medium\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_size_multiplier_factor\",\"setting\":\"2.0\",\"unit\":null,\"category\":\"COPY / Factor for estimating COPY commit size\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"real\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"2.0\",\"reset_val\":\"2.0\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_commit_timeout\",\"setting\":\"300\",\"unit\":null,\"category\":\"COPY / Timeout in seconds for commits for COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"300\",\"reset_val\":\"300\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_atomic_commit_size\",\"setting\":\"100000000\",\"unit\":null,\"category\":\"COPY / Max number of bytes in an atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"100000000\",\"reset_val\":\"100000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_atomic_mutations\",\"setting\":\"20000\",\"unit\":null,\"category\":\"COPY / Max number of mutations for atomic COPY operations\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"20000\",\"reset_val\":\"20000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_non_atomic_commit_size\",\"setting\":\"5000000\",\"unit\":null,\"category\":\"COPY / The max number of bytes per commit in a non-atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"5000000\",\"reset_val\":\"5000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_max_parallelism\",\"setting\":\"128\",\"unit\":null,\"category\":\"COPY / Max concurrent transactions for a non-atomic COPY operation\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"128\",\"reset_val\":\"128\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_pipe_buffer_size\",\"setting\":\"65536\",\"unit\":null,\"category\":\"COPY / Buffer size for incoming COPY data messages\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"internal\",\"vartype\":\"integer\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"100000000\",\"reset_val\":\"100000000\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.copy_upsert\",\"setting\":\"off\",\"unit\":null,\"category\":\"COPY / Use Upsert instead of Insert for COPY\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.ddl_transaction_mode\",\"setting\":\"Batch\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"Single\", \"Batch\", \"AutocommitImplicitTransaction\", \"AutocommitExplicitTransaction\"],\"boot_val\":\"Batch\",\"reset_val\":\"Batch\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.emulate_pg_class_tables\",\"setting\":\"on\",\"unit\":null,\"category\":\"PGAdapter Options Emulate pg_class and related tables using common table expressions and textual OIDs\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.force_autocommit\",\"setting\":\"off\",\"unit\":null,\"category\":\"PGAdapter Options Execute all statements in autocommit mode\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.replace_pg_catalog_tables\",\"setting\":\"false\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"on\",\"reset_val\":\"on\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"spanner.well_known_client\",\"setting\":\"UNSPECIFIED\",\"unit\":null,\"category\":\"PGAdapter Options\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"backend\",\"vartype\":\"string\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"UNSPECIFIED\",\"reset_val\":\"UNSPECIFIED\",\"source\":\"default\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"transaction_isolation\",\"setting\":\"serializable\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"enum\",\"min_val\":null,\"max_val\":null,\"enum_vals\":[\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"],\"boot_val\":\"serializable\",\"reset_val\":\"serializable\",\"source\":\"override\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb,\n"
        + "'{\"name\":\"transaction_read_only\",\"setting\":\"off\",\"unit\":null,\"category\":\"Client Connection Defaults / Statement Behavior\",\"short_desc\":null,\"extra_desc\":null,\"context\":\"user\",\"vartype\":\"bool\",\"min_val\":null,\"max_val\":null,\"enum_vals\":null,\"boot_val\":\"off\",\"reset_val\":\"off\",\"source\":\"override\",\"sourcefile\":null,\"sourceline\":null,\"pending_restart\":false}'::jsonb\n"
        + "]) t\n"
        + "),\n"
        + "pg_settings_names_ as (\n"
        + "select name from pg_settings_inmem_\n"
        + "union\n"
        + "select name from pg_catalog.pg_settings\n"
        + "),\n"
        + "pg_settings as (\n"
        + "select n.name, coalesce(s1.setting, s2.setting) as setting,coalesce(s1.unit, s2.unit) as unit,coalesce(s1.category, s2.category) as category,coalesce(s1.short_desc, s2.short_desc) as short_desc,coalesce(s1.extra_desc, s2.extra_desc) as extra_desc,coalesce(s1.context, s2.context) as context,coalesce(s1.vartype, s2.vartype) as vartype,coalesce(s1.source, s2.source) as source,coalesce(s1.min_val, s2.min_val) as min_val,coalesce(s1.max_val, s2.max_val) as max_val,coalesce(s1.enumvals, s2.enumvals) as enumvals,coalesce(s1.boot_val, s2.boot_val) as boot_val,coalesce(s1.reset_val, s2.reset_val) as reset_val,coalesce(s1.sourcefile, s2.sourcefile) as sourcefile,coalesce(s1.sourceline, s2.sourceline) as sourceline,coalesce(s1.pending_restart, s2.pending_restart) as pending_restart\n"
        + "from pg_settings_names_ n\n"
        + "left join pg_settings_inmem_ s1 using (name)\n"
        + "left join pg_catalog.pg_settings s2 using (name)\n"
        + "order by name\n"
        + ")\n";
  }

  @Test
  public void testGeneratePGSettingsCte() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));

    String cte = state.generatePGSettingsCte();

    assertEquals(getDefaultSessionStateExpression(), cte);
  }

  @Test
  public void testAddSessionState() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement = Statement.of("select * from pg_settings");

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertEquals(
        "with " + getDefaultSessionStateExpression() + "\n" + statement.getSql(),
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithParameters() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement =
        Statement.newBuilder("select * from pg_settings where name=$1")
            .bind("p1")
            .to("some-name")
            .build();

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertEquals(
        Statement.newBuilder(
                "with " + getDefaultSessionStateExpression() + "\n" + statement.getSql())
            .bind("p1")
            .to("some-name")
            .build(),
        withSessionState);
  }

  @Test
  public void testAddSessionStateWithoutPgSettings() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement = Statement.of("select * from some_table");

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertSame(statement, withSessionState);
  }

  @Test
  public void testAddSessionStateWithComments() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement = Statement.of("/* This comment is preserved */ select * from pg_settings");

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertEquals(
        "with " + getDefaultSessionStateExpression() + "\n" + statement.getSql(),
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithExistingCTE() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement =
        Statement.of(
            "with my_cte as (select col1, col2 from foo) select * from pg_settings inner join my_cte on my_cte.col1=pg_settings.name");

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertEquals(
        "with "
            + getDefaultSessionStateExpression()
            + ",\n my_cte as (select col1, col2 from foo) select * from pg_settings inner join my_cte on my_cte.col1=pg_settings.name",
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithCommentsAndExistingCTE() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state, WellKnownClient.UNSPECIFIED);
    Statement statement =
        Statement.of(
            "/* This comment is preserved */ with foo as (select * from bar)\nselect * from pg_settings");

    Statement withSessionState =
        pgCatalog.replacePgCatalogTables(statement, statement.getSql().toLowerCase(Locale.ENGLISH));

    assertEquals(
        "with "
            + getDefaultSessionStateExpression()
            + ",\n/* This comment is preserved */  foo as (select * from bar)\nselect * from pg_settings",
        withSessionState.getSql());
  }

  @Test
  public void testGetBoolSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertFalse(state.getBoolSetting("spanner", "unknown_setting", false));
    assertTrue(state.getBoolSetting("spanner", "unknown_setting", true));

    state.set("spanner", "custom_setting", "on");
    assertTrue(state.getBoolSetting("spanner", "custom_setting", false));
    state.set("spanner", "custom_setting", "off");
    assertFalse(state.getBoolSetting("spanner", "custom_setting", true));

    state.set("spanner", "custom_setting", "foo");
    assertFalse(state.getBoolSetting("spanner", "custom_setting", false));
    state.set("spanner", "custom_setting", "foo");
    assertTrue(state.getBoolSetting("spanner", "custom_setting", true));
  }

  @Test
  public void testGetIntegerSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals(100, state.getIntegerSetting("spanner", "unknown_setting", 100));
    assertEquals(0, state.getIntegerSetting("spanner", "unknown_setting", 0));

    state.set("spanner", "custom_setting", "200");
    assertEquals(200, state.getIntegerSetting("spanner", "custom_setting", 100));
    state.set("spanner", "custom_setting", "-200");
    assertEquals(-200, state.getIntegerSetting("spanner", "custom_setting", 100));

    state.set("spanner", "custom_setting", "foo");
    assertEquals(100, state.getIntegerSetting("spanner", "custom_setting", 100));
    state.set("spanner", "custom_setting", "bar");
    assertEquals(0, state.getIntegerSetting("spanner", "custom_setting", 0));
  }

  @Test
  public void testGetFloatSetting() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertEquals(1.1f, state.getFloatSetting("spanner", "unknown_setting", 1.1f), 0.0f);
    assertEquals(0.0f, state.getFloatSetting("spanner", "unknown_setting", 0.0f), 0.0f);

    state.set("spanner", "custom_setting", "2");
    assertEquals(2f, state.getFloatSetting("spanner", "custom_setting", 1f), 0.0f);
    state.set("spanner", "custom_setting", "-2.9");
    assertEquals(-2.9f, state.getFloatSetting("spanner", "custom_setting", 1.5f), 0.0f);

    state.set("spanner", "custom_setting", "foo");
    assertEquals(4.4f, state.getFloatSetting("spanner", "custom_setting", 4.4f), 0.0f);
    state.set("spanner", "custom_setting", "bar");
    assertEquals(0f, state.getFloatSetting("spanner", "custom_setting", 0f), 0.0f);
  }

  @Test
  public void testTryGet() {
    assertNull(
        tryGet(
            () -> {
              throw new IllegalArgumentException();
            }));
    assertEquals("test", tryGet(() -> "test"));
  }

  @Test
  public void testTryGetFirstNonNull() {
    assertEquals("default", tryGetFirstNonNull("default"));
    assertEquals("test", tryGetFirstNonNull("default", () -> "test"));
    assertEquals("test", tryGetFirstNonNull("default", () -> null, () -> "test"));
    assertEquals("default", tryGetFirstNonNull("default", () -> null, () -> null));
  }

  @Test
  public void testIsReplacePgCatalogTables_noDefault() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    assertFalse(state.isReplacePgCatalogTables());
  }

  @Test
  public void testIsReplacePgCatalogTables_defaultFalse() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(optionsMetadata.replacePgCatalogTables()).thenReturn(false);
    SessionState state =
        new SessionState(
            ImmutableMap.of(
                "spanner.replace_pg_catalog_tables",
                new PGSetting("spanner", "replace_pg_catalog_tables")),
            optionsMetadata);
    assertFalse(state.isReplacePgCatalogTables());
  }

  @Test
  public void testIsReplacePgCatalogTables_defaultTrue() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(optionsMetadata.replacePgCatalogTables()).thenReturn(true);
    SessionState state =
        new SessionState(
            ImmutableMap.of(
                "spanner.replace_pg_catalog_tables",
                new PGSetting("spanner", "replace_pg_catalog_tables")),
            optionsMetadata);
    assertTrue(state.isReplacePgCatalogTables());
  }

  @Test
  public void testIsReplacePgCatalogTables_resetVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "replace_pg_catalog_tables",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* resetVal = */ "off",
            null,
            null,
            false);
    SessionState state =
        new SessionState(
            ImmutableMap.of("spanner.replace_pg_catalog_tables", setting), optionsMetadata);
    state.set("spanner", "replace_pg_catalog_tables", null);

    assertFalse(state.isReplacePgCatalogTables());
  }

  @Test
  public void testIsReplacePgCatalogTables_bootVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "replace_pg_catalog_tables",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* bootVal = */ "off",
            null,
            null,
            null,
            false);
    SessionState state =
        new SessionState(
            ImmutableMap.of("spanner.replace_pg_catalog_tables", setting), optionsMetadata);
    state.set("spanner", "replace_pg_catalog_tables", null);

    assertFalse(state.isReplacePgCatalogTables());
  }

  @Test
  public void testIsEmulatePgClassTables_resetVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "emulate_pg_class_tables",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* resetVal = */ "off",
            null,
            null,
            false);
    SessionState state =
        new SessionState(
            ImmutableMap.of("spanner.emulate_pg_class_tables", setting), optionsMetadata);
    state.set("spanner", "emulate_pg_class_tables", null);

    assertFalse(state.isEmulatePgClassTables());
  }

  @Test
  public void testIsEmulatePgClassTables_bootVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "emulate_pg_class_tables",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* bootVal = */ "off",
            null,
            null,
            null,
            false);
    SessionState state =
        new SessionState(
            ImmutableMap.of("spanner.emulate_pg_class_tables", setting), optionsMetadata);
    state.set("spanner", "emulate_pg_class_tables", null);

    assertFalse(state.isEmulatePgClassTables());
  }

  @Test
  public void testIsEmulatePgClassTables_NoValue() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    SessionState state = new SessionState(ImmutableMap.of(), optionsMetadata);
    state.getSettings().clear();
    assertFalse(state.isEmulatePgClassTables());
  }

  @Test
  public void testCopyCommitPriority() {
    SessionState state = new SessionState(ImmutableMap.of(), mock(OptionsMetadata.class));
    CopySettings copySettings = new CopySettings(state);

    assertEquals(RpcPriority.MEDIUM, copySettings.getCommitPriority());

    state.set("spanner", "copy_commit_priority", "high");
    assertEquals(RpcPriority.HIGH, copySettings.getCommitPriority());
    state.set("spanner", "copy_commit_priority", "Low");
    assertEquals(RpcPriority.LOW, copySettings.getCommitPriority());

    PGException exception =
        assertThrows(PGException.class, () -> state.set("spanner", "copy_commit_priority", "foo"));
    assertEquals(
        "invalid value for parameter \"spanner.copy_commit_priority\": \"foo\"",
        exception.getMessage());
    assertEquals("Available values: low, medium, high.", exception.getHints());
  }

  @Test
  public void testDdlTransactionMode_noDefault() {
    SessionState state = new SessionState(ImmutableMap.of(), mock(OptionsMetadata.class));
    assertEquals(DdlTransactionMode.Batch, state.getDdlTransactionMode());
  }

  @Test
  public void testDdlTransactionMode_defaultSingle() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(optionsMetadata.getDdlTransactionMode()).thenReturn(DdlTransactionMode.Single);
    SessionState state =
        new SessionState(
            ImmutableMap.of(
                "spanner.ddl_transaction_mode", new PGSetting("spanner", "ddl_transaction_mode")),
            optionsMetadata);
    assertEquals(DdlTransactionMode.Single, state.getDdlTransactionMode());
  }

  @Test
  public void testDdlTransactionMode_defaultExplicit() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    when(optionsMetadata.getDdlTransactionMode())
        .thenReturn(DdlTransactionMode.AutocommitExplicitTransaction);
    SessionState state =
        new SessionState(
            ImmutableMap.of(
                "spanner.ddl_transaction_mode", new PGSetting("spanner", "ddl_transaction_mode")),
            optionsMetadata);
    assertEquals(DdlTransactionMode.AutocommitExplicitTransaction, state.getDdlTransactionMode());
  }

  @Test
  public void testDdlTransactionMode_resetVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "ddl_transaction_mode",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* resetVal = */ "Single",
            null,
            null,
            false);
    SessionState state =
        new SessionState(ImmutableMap.of("spanner.ddl_transaction_mode", setting), optionsMetadata);
    state.set("spanner", "ddl_transaction_mode", null);

    assertEquals(DdlTransactionMode.Single, state.getDdlTransactionMode());
  }

  @Test
  public void testDdlTransactionMode_bootVal() {
    OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
    PGSetting setting =
        new PGSetting(
            "spanner",
            "ddl_transaction_mode",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            /* bootVal = */ "Single",
            null,
            null,
            null,
            false);
    SessionState state =
        new SessionState(ImmutableMap.of("spanner.ddl_transaction_mode", setting), optionsMetadata);
    state.set("spanner", "ddl_transaction_mode", null);

    assertEquals(DdlTransactionMode.Single, state.getDdlTransactionMode());
  }

  @Test
  public void testGetDefaultTimeZone() {
    Map<String, PGSetting> originalSettings = ImmutableMap.copyOf(SessionState.SERVER_SETTINGS);
    SessionState.SERVER_SETTINGS.remove("TimeZone");
    try {
      OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
      SessionState state = new SessionState(ImmutableMap.of(), optionsMetadata);
      assertEquals(TimeZone.getDefault().toZoneId(), state.getTimezone());
    } finally {
      SessionState.SERVER_SETTINGS.putAll(originalSettings);
    }
  }

  @Test
  public void testTimeZoneResetVal() {
    Map<String, PGSetting> originalSettings = ImmutableMap.copyOf(SessionState.SERVER_SETTINGS);
    SessionState.SERVER_SETTINGS.put(
        "TimeZone",
        new PGSetting(
            null,
            "TimeZone",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "Europe/Oslo",
            "America/New_York",
            null,
            null,
            false));
    try {
      OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
      SessionState state = new SessionState(ImmutableMap.of(), optionsMetadata);
      assertEquals("America/New_York", state.getTimezone().getId());
    } finally {
      SessionState.SERVER_SETTINGS.putAll(originalSettings);
    }
  }

  @Test
  public void testTimeZoneBootVal() {
    Map<String, PGSetting> originalSettings = ImmutableMap.copyOf(SessionState.SERVER_SETTINGS);
    SessionState.SERVER_SETTINGS.put(
        "TimeZone",
        new PGSetting(
            null,
            "TimeZone",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "Europe/Oslo",
            null,
            null,
            null,
            false));
    try {
      OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
      SessionState state = new SessionState(ImmutableMap.of(), optionsMetadata);
      assertEquals("Europe/Oslo", state.getTimezone().getId());
    } finally {
      SessionState.SERVER_SETTINGS.putAll(originalSettings);
    }
  }

  @Test
  public void testGetInvalidTimeZone() {
    Map<String, PGSetting> originalSettings = ImmutableMap.copyOf(SessionState.SERVER_SETTINGS);
    SessionState.SERVER_SETTINGS.put(
        "TimeZone",
        new PGSetting(
            null,
            "TimeZone",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "foo/bar",
            null,
            null,
            null,
            null,
            null,
            null,
            false));
    try {
      OptionsMetadata optionsMetadata = mock(OptionsMetadata.class);
      SessionState state = new SessionState(ImmutableMap.of(), optionsMetadata);
      assertEquals(TimeZone.getDefault().toZoneId(), state.getTimezone());
    } finally {
      SessionState.SERVER_SETTINGS.putAll(originalSettings);
    }
  }
}
