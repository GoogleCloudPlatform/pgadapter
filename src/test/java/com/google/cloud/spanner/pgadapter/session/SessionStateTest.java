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
}
