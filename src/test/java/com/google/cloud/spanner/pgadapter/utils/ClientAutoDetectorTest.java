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

package com.google.cloud.spanner.pgadapter.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClientAutoDetectorTest {
  @Test
  public void testJdbc() {
    // The JDBC driver will **always** include these startup parameters in exactly this order.
    // The values of 'client_encoding' and 'DateStyle' are fixed.
    // Any other permutation of the first 5 parameters should fail the JDBC check.
    assertEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "client_encoding", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "UTF8",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone")));
    // Additional parameters are allowed.
    assertEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of(
                "user", "database", "client_encoding", "DateStyle", "TimeZone", "some-other-param"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "UTF8",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone",
                "some-other-param", "param-value")));

    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "client_encoding", "DateStyle"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "UTF8",
                "DateStyle", "ISO")));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "client_encoding", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "UTF8",
                "TimeZone", "some-time-zone")));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone")));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "client_encoding", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "client_encoding", "UTF8",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone")));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("database", "client_encoding", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "database", "some-database",
                "client_encoding", "UTF8",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone")));
    // A different value (even if it's only a difference in case) for client_encoding or DateStyle
    // will also fail the detection of the JDBC driver.
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "client_encoding", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "utf8",
                "DateStyle", "ISO",
                "TimeZone", "some-time-zone")));
    assertNotEquals(
        WellKnownClient.JDBC,
        ClientAutoDetector.detectClient(
            ImmutableList.of("user", "database", "client_encoding", "DateStyle", "TimeZone"),
            ImmutableMap.of(
                "user", "some-user",
                "database", "some-database",
                "client_encoding", "UTF8",
                "DateStyle", "iso",
                "TimeZone", "some-time-zone")));
    ImmutableList<String> keys =
        ImmutableList.of("user", "database", "client_encoding", "DateStyle", "TimeZone");
    for (int i = 0; i < keys.size(); i++) {
      List<String> list = new ArrayList<>(keys);
      list.set(i, "foo");
      assertNotEquals(
          WellKnownClient.JDBC,
          ClientAutoDetector.detectClient(
              list,
              ImmutableMap.of(
                  "user", "some-user",
                  "database", "some-database",
                  "client_encoding", "UTF8",
                  "DateStyle", "ISO",
                  "TimeZone", "some-time-zone")));
    }
  }

  @Test
  public void testPsql() {
    // Psql always includes itself as the application_name.
    assertEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "psql")));
    assertEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name", "some-param"),
            ImmutableMap.of(
                "application_name", "psql",
                "some-param", "some-value")));
    assertEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param1", "application_name", "some-param2"),
            ImmutableMap.of(
                "some-param1", "some-value1",
                "application_name", "psql",
                "some-param2", "some-value2")));
    assertNotEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param1"), ImmutableMap.of("some-param1", "some-value1")));
    assertNotEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "JDBC")));
    assertNotEquals(
        WellKnownClient.PSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "PSQL")));
  }

  @Test
  public void testPgx() {
    assertNotEquals(
        WellKnownClient.PGX,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.PGX,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param"), ImmutableMap.of("some-param", "some-value")));
  }
}
