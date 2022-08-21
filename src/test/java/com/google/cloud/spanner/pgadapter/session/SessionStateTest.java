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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog;
import java.util.List;
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
    assertEquals(345, allSettings.size());
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
    assertEquals(347, allSettings.size());

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
    SpannerException exception =
        assertThrows(SpannerException.class, () -> state.set(null, "bytea_output", "random_value"));
    assertEquals(
        "INVALID_ARGUMENT: invalid value for parameter \"bytea_output\": \"random_value\"",
        exception.getMessage());
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
        + "select 'DateStyle' as name, 'ISO, MDY' as setting, null as unit, 'Client Connection Defaults / Locale and Formatting' as category, null as short_desc, null as extra_desc, 'user' as context, 'string' as vartype, null as min_val, null as max_val, null::text[] as enumvals, 'ISO, MDY' as boot_val, 'ISO, MDY' as reset_val, 'configuration file' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'TimeZone' as name, 'Europe/Berlin' as setting, null as unit, 'Client Connection Defaults / Locale and Formatting' as category, null as short_desc, null as extra_desc, 'user' as context, 'string' as vartype, null as min_val, null as max_val, null::text[] as enumvals, 'GMT' as boot_val, 'Europe/Berlin' as reset_val, 'configuration file' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'application_name' as name, null as setting, null as unit, 'Reporting and Logging / What to Log' as category, null as short_desc, null as extra_desc, 'user' as context, 'string' as vartype, null as min_val, null as max_val, null::text[] as enumvals, '' as boot_val, null as reset_val, 'client' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'bytea_output' as name, 'hex' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'enum' as vartype, null as min_val, null as max_val, '{\"escape\", \"hex\"}'::text[] as enumvals, 'hex' as boot_val, 'hex' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'default_transaction_isolation' as name, 'serializable' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'enum' as vartype, null as min_val, null as max_val, '{\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"}'::text[] as enumvals, 'serializable' as boot_val, 'serializable' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'default_transaction_read_only' as name, 'off' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'bool' as vartype, null as min_val, null as max_val, null::text[] as enumvals, 'off' as boot_val, 'off' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'extra_float_digits' as name, '1' as setting, null as unit, 'Client Connection Defaults / Locale and Formatting' as category, null as short_desc, null as extra_desc, 'user' as context, 'integer' as vartype, '-15' as min_val, '3' as max_val, null::text[] as enumvals, '1' as boot_val, '1' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'max_connections' as name, '100' as setting, null as unit, 'Connections and Authentication / Connection Settings' as category, null as short_desc, null as extra_desc, 'postmaster' as context, 'integer' as vartype, '1' as min_val, '262143' as max_val, null::text[] as enumvals, '100' as boot_val, '100' as reset_val, 'configuration file' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'max_index_keys' as name, '16' as setting, null as unit, 'Preset Options' as category, null as short_desc, null as extra_desc, 'internal' as context, 'integer' as vartype, '16' as min_val, '16' as max_val, null::text[] as enumvals, '16' as boot_val, '16' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'port' as name, '5432' as setting, null as unit, 'Connections and Authentication / Connection Settings' as category, null as short_desc, null as extra_desc, 'postmaster' as context, 'integer' as vartype, '1' as min_val, '65535' as max_val, null::text[] as enumvals, '5432' as boot_val, '5432' as reset_val, 'configuration file' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'search_path' as name, 'public' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'string' as vartype, null as min_val, null as max_val, null::text[] as enumvals, 'public' as boot_val, 'public' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'server_version' as name, null as setting, null as unit, 'Preset Options' as category, null as short_desc, null as extra_desc, 'internal' as context, 'string' as vartype, null as min_val, null as max_val, null::text[] as enumvals, '14.1 (Debian 14.1-1.pgdg110+1)' as boot_val, '14.1 (Debian 14.1-1.pgdg110+1)' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'server_version_num' as name, null as setting, null as unit, 'Preset Options' as category, null as short_desc, null as extra_desc, 'internal' as context, 'integer' as vartype, '140001' as min_val, '140001' as max_val, null::text[] as enumvals, '140001' as boot_val, '140001' as reset_val, 'default' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'transaction_isolation' as name, 'serializable' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'enum' as vartype, null as min_val, null as max_val, '{\"serializable\", \"repeatable read\", \"read committed\", \"read uncommitted\"}'::text[] as enumvals, 'serializable' as boot_val, 'serializable' as reset_val, 'override' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
        + "union all\n"
        + "select 'transaction_read_only' as name, 'off' as setting, null as unit, 'Client Connection Defaults / Statement Behavior' as category, null as short_desc, null as extra_desc, 'user' as context, 'bool' as vartype, null as min_val, null as max_val, null::text[] as enumvals, 'off' as boot_val, 'off' as reset_val, 'override' as source, null as sourcefile, null::bigint as sourceline, 'f'::boolean as pending_restart\n"
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
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement = Statement.of("select * from pg_settings");

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertEquals(
        "with " + getDefaultSessionStateExpression() + statement.getSql(),
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithParameters() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement =
        Statement.newBuilder("select * from pg_settings where name=$1")
            .bind("p1")
            .to("some-name")
            .build();

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertEquals(
        Statement.newBuilder("with " + getDefaultSessionStateExpression() + statement.getSql())
            .bind("p1")
            .to("some-name")
            .build(),
        withSessionState);
  }

  @Test
  public void testAddSessionStateWithoutPgSettings() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement = Statement.of("select * from some_table");

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertSame(statement, withSessionState);
  }

  @Test
  public void testAddSessionStateWithComments() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement = Statement.of("/* This comment is preserved */ select * from pg_settings");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertEquals(
        "with " + getDefaultSessionStateExpression() + statement.getSql(),
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithExistingCTE() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement =
        Statement.of(
            "with my_cte as (select col1, col2 from foo) select * from pg_settings inner join my_cte on my_cte.col1=pg_settings.name");

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertEquals(
        "with "
            + getDefaultSessionStateExpression()
            + ",  my_cte as (select col1, col2 from foo) select * from pg_settings inner join my_cte on my_cte.col1=pg_settings.name",
        withSessionState.getSql());
  }

  @Test
  public void testAddSessionStateWithCommentsAndExistingCTE() {
    SessionState state = new SessionState(mock(OptionsMetadata.class));
    PgCatalog pgCatalog = new PgCatalog(state);
    Statement statement =
        Statement.of(
            "/* This comment is preserved */ with foo as (select * from bar)\nselect * from pg_settings");
    ParsedStatement parsedStatement =
        AbstractStatementParser.getInstance(Dialect.POSTGRESQL).parse(statement);

    Statement withSessionState = pgCatalog.replacePgCatalogTables(statement);

    assertEquals(
        "with "
            + getDefaultSessionStateExpression()
            + ", /* This comment is preserved */ foo as (select * from bar)\nselect * from pg_settings",
        withSessionState.getSql());
  }
}
