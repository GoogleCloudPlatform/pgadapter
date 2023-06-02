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

import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.DEFAULT_LOCAL_STATEMENTS;
import static com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.EMPTY_LOCAL_STATEMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.session.PGSetting;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.wireoutput.NoticeResponse;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClientAutoDetectorTest {

  @Test
  public void testUnspecified() {
    WellKnownClient.UNSPECIFIED.reset();

    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param"), ImmutableMap.of("some-param", "some-value")));

    ConnectionHandler connection = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(server.getOptions()).thenReturn(options);
    when(connection.getServer()).thenReturn(server);

    when(options.useDefaultLocalStatements()).thenReturn(true);
    assertEquals(
        DEFAULT_LOCAL_STATEMENTS, WellKnownClient.UNSPECIFIED.getLocalStatements(connection));

    when(options.useDefaultLocalStatements()).thenReturn(false);
    assertEquals(ImmutableList.of(), WellKnownClient.UNSPECIFIED.getLocalStatements(connection));

    assertEquals(
        ImmutableList.of(), WellKnownClient.UNSPECIFIED.createStartupNoticeResponses(connection));
    assertEquals(
        ImmutableList.of(), WellKnownClient.UNSPECIFIED.getErrorHints(mock(PGException.class)));
    assertEquals(ImmutableMap.of(), WellKnownClient.UNSPECIFIED.getDefaultParameters());
  }

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

    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    for (boolean useDefaultLocalStatements : new boolean[] {true, false}) {
      when(options.useDefaultLocalStatements()).thenReturn(useDefaultLocalStatements);
      if (useDefaultLocalStatements) {
        assertEquals(
            DEFAULT_LOCAL_STATEMENTS, WellKnownClient.JDBC.getLocalStatements(connectionHandler));
      } else {
        assertEquals(
            EMPTY_LOCAL_STATEMENTS, WellKnownClient.JDBC.getLocalStatements(connectionHandler));
      }
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

    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    assertEquals(
        ImmutableList.of(new ListDatabasesStatement(connectionHandler)),
        WellKnownClient.PSQL.getLocalStatements(connectionHandler));
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

    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    for (boolean useDefaultLocalStatements : new boolean[] {true, false}) {
      when(options.useDefaultLocalStatements()).thenReturn(useDefaultLocalStatements);
      if (useDefaultLocalStatements) {
        assertEquals(
            DEFAULT_LOCAL_STATEMENTS, WellKnownClient.PGX.getLocalStatements(connectionHandler));
      } else {
        assertEquals(
            EMPTY_LOCAL_STATEMENTS, WellKnownClient.PGX.getLocalStatements(connectionHandler));
      }
    }
  }

  @Test
  public void testNpgsql() {
    assertNotEquals(
        WellKnownClient.NPGSQL,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.NPGSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param"), ImmutableMap.of("some-param", "some-value")));

    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    for (boolean useDefaultLocalStatements : new boolean[] {true, false}) {
      when(options.useDefaultLocalStatements()).thenReturn(useDefaultLocalStatements);
      if (useDefaultLocalStatements) {
        assertEquals(
            DEFAULT_LOCAL_STATEMENTS, WellKnownClient.NPGSQL.getLocalStatements(connectionHandler));
      } else {
        assertEquals(
            EMPTY_LOCAL_STATEMENTS, WellKnownClient.NPGSQL.getLocalStatements(connectionHandler));
      }
    }
    assertEquals(
        WellKnownClient.NPGSQL,
        ClientAutoDetector.detectClient(
            ImmutableList.of(),
            ImmutableList.of(
                Statement.of(
                    "SELECT version();\n"
                        + "\n"
                        + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"))));
    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            ImmutableList.of(), ImmutableList.of(Statement.of("SELECT version()"))));
    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            ImmutableList.of(),
            ImmutableList.of(
                Statement.of(
                    "SELECT version();\n"
                        + "\n"
                        + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"),
                Statement.of("SELECT version();"))));
  }

  @Test
  public void testSQLAlchemy2() {
    assertNotEquals(
        WellKnownClient.SQLALCHEMY2,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.SQLALCHEMY2,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param"), ImmutableMap.of("some-param", "some-value")));

    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    ProxyServer server = mock(ProxyServer.class);
    OptionsMetadata options = mock(OptionsMetadata.class);
    when(connectionHandler.getServer()).thenReturn(server);
    when(server.getOptions()).thenReturn(options);
    for (boolean useDefaultLocalStatements : new boolean[] {true, false}) {
      when(options.useDefaultLocalStatements()).thenReturn(useDefaultLocalStatements);
      if (useDefaultLocalStatements) {
        assertEquals(
            DEFAULT_LOCAL_STATEMENTS,
            WellKnownClient.SQLALCHEMY2.getLocalStatements(connectionHandler));
      } else {
        assertEquals(
            EMPTY_LOCAL_STATEMENTS,
            WellKnownClient.SQLALCHEMY2.getLocalStatements(connectionHandler));
      }
    }
    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            Collections.emptyList(),
            ImmutableList.of(Statement.of("select pg_catalog.version()"))));

    ParseMessage parseMessage = mock(ParseMessage.class);
    when(parseMessage.getSql()).thenReturn("BEGIN");
    assertEquals(
        WellKnownClient.SQLALCHEMY2,
        ClientAutoDetector.detectClient(
            ImmutableList.of(parseMessage),
            ImmutableList.of(Statement.of("select pg_catalog.version()"))));
    when(parseMessage.getSql()).thenReturn("begin");
    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            ImmutableList.of(parseMessage),
            ImmutableList.of(Statement.of("select pg_catalog.version()"))));

    assertEquals(
        WellKnownClient.UNSPECIFIED,
        ClientAutoDetector.detectClient(
            Collections.emptyList(), ImmutableList.of(Statement.of("SELECT version()"))));
  }

  @Test
  public void testPgbench() {
    // pgbench always includes itself as the application_name.
    assertEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "pgbench")));
    assertEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name", "some-param"),
            ImmutableMap.of(
                "application_name", "pgbench",
                "some-param", "some-value")));
    assertEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param1", "application_name", "some-param2"),
            ImmutableMap.of(
                "some-param1", "some-value1",
                "application_name", "pgbench",
                "some-param2", "some-value2")));
    assertNotEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(ImmutableList.of(), ImmutableMap.of()));
    assertNotEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("some-param1"), ImmutableMap.of("some-param1", "some-value1")));
    assertNotEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "JDBC")));
    assertNotEquals(
        WellKnownClient.PGBENCH,
        ClientAutoDetector.detectClient(
            ImmutableList.of("application_name"), ImmutableMap.of("application_name", "PGBENCH")));

    WellKnownClient.PGBENCH.reset();
    ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
    when(connectionHandler.getConnectionMetadata()).thenReturn(mock(ConnectionMetadata.class));
    ImmutableList<NoticeResponse> startupNotices =
        WellKnownClient.PGBENCH.createStartupNoticeResponses(connectionHandler);
    assertEquals(1, startupNotices.size());
    assertEquals("Detected connection from pgbench", startupNotices.get(0).getMessage());
    assertEquals(ClientAutoDetector.PGBENCH_USAGE_HINT + "\n", startupNotices.get(0).getHint());
  }

  @Test
  public void testSetting() {
    assertEquals(WellKnownClient.UNSPECIFIED, ClientAutoDetector.detectClient(null));
    assertEquals(
        WellKnownClient.UNSPECIFIED, ClientAutoDetector.detectClient(mock(PGSetting.class)));

    PGSetting jdbc = mock(PGSetting.class);
    when(jdbc.getSetting()).thenReturn("jdbc");
    assertEquals(WellKnownClient.JDBC, ClientAutoDetector.detectClient(jdbc));

    PGSetting unspecified = mock(PGSetting.class);
    when(unspecified.getSetting()).thenReturn("unspecified");
    assertEquals(WellKnownClient.UNSPECIFIED, ClientAutoDetector.detectClient(unspecified));

    PGSetting foo = mock(PGSetting.class);
    when(foo.getSetting()).thenReturn("foo");
    assertEquals(WellKnownClient.UNSPECIFIED, ClientAutoDetector.detectClient(foo));

    try {
      WellKnownClient.DEFAULT_UNSPECIFIED.set(false);
      assertThrows(IllegalStateException.class, () -> ClientAutoDetector.detectClient(foo));
    } finally {
      WellKnownClient.DEFAULT_UNSPECIFIED.set(true);
    }
  }
}
