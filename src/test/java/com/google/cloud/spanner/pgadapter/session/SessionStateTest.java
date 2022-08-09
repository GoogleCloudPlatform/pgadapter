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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
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
    assertEquals(308, allSettings.size());
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
    assertEquals(310, allSettings.size());

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
}
