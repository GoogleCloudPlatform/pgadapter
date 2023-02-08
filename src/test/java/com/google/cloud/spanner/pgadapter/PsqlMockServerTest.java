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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.CopyToStatement;
import com.google.cloud.spanner.pgadapter.wireprotocol.SSLMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PsqlMockServerTest extends AbstractMockServerTest {

  @BeforeClass
  public static void checkPsqlAvailable() {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        new MockSpannerServiceImpl(), "d", ImmutableList.of("-ddl=AutocommitExplicitTransaction"));
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

  @Test
  public void testCopyInBatchPsql() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    File file = new File("src/test/resources/ddl-batch.sql");
    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          "-h",
          "localhost",
          "-p",
          String.valueOf(pgServer.getLocalPort()),
          "-f",
          file.getAbsolutePath()
        };
    builder.command(psqlCommand);
    Process process = builder.start();
    String errors;
    String output;

    try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errorReader =
            new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
      output = reader.lines().collect(Collectors.joining("\n"));
    }

    assertEquals("", errors);
    assertEquals("BEGIN\nCREATE\nCREATE\nCOMMIT", output);
    int res = process.waitFor();
    assertEquals(0, res);

    List<UpdateDatabaseDdlRequest> requests =
        mockDatabaseAdmin.getRequests().stream()
            .filter(msg -> msg instanceof UpdateDatabaseDdlRequest)
            .map(msg -> (UpdateDatabaseDdlRequest) msg)
            .collect(Collectors.toList());
    assertEquals(1, requests.size());
    UpdateDatabaseDdlRequest request = requests.get(0);
    assertEquals(2, request.getStatementsCount());
    assertTrue(request.getStatements(0).startsWith("CREATE TABLE databasechangeloglock ("));
    assertTrue(request.getStatements(1).startsWith("CREATE TABLE databasechangelog ("));
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

    assertTrue(errors.contains("server does not support SSL, but SSL was required"));
    assertEquals("", output);
    int res = process.waitFor();
    assertNotEquals(0, res);

    assertEquals(
        1L, pgServer.getDebugMessages().stream().filter(m -> m instanceof SSLMessage).count());
  }

  @Test
  public void testCopyToBinaryPsql() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());

    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          "-h",
          "localhost",
          "-p",
          String.valueOf(pgServer.getLocalPort()),
          "-c",
          "COPY (SELECT 1) TO STDOUT (FORMAT BINARY)"
        };
    builder.command(psqlCommand);
    Process process = builder.start();
    String errors;

    try (BufferedReader errorReader =
        new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
    }
    byte[] copyOutput = ByteStreams.toByteArray(process.getInputStream());

    assertEquals("", errors);
    int res = process.waitFor();
    assertEquals(0, res);

    ByteArrayOutputStream byteArrayOutputStream =
        new ByteArrayOutputStream(CopyToStatement.COPY_BINARY_HEADER.length);
    DataOutputStream expectedOutputStream = new DataOutputStream(byteArrayOutputStream);
    expectedOutputStream.write(CopyToStatement.COPY_BINARY_HEADER);
    expectedOutputStream.writeInt(0); // flags
    expectedOutputStream.writeInt(0); // header extension area length
    expectedOutputStream.writeShort(1); // Column count
    expectedOutputStream.writeInt(8); // Value length
    expectedOutputStream.writeLong(1L); // Column value
    expectedOutputStream.writeShort(-1); // Column count == -1 means end of data.

    assertArrayEquals(byteArrayOutputStream.toByteArray(), copyOutput);
  }

  @Test
  public void testCopyToBinaryPsqlEmptyTable() throws Exception {
    assumeTrue("This test requires psql to be installed", isPsqlAvailable());
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of("select * from all_types"),
            ALL_TYPES_RESULTSET.toBuilder().clearRows().build()));

    ProcessBuilder builder = new ProcessBuilder();
    String[] psqlCommand =
        new String[] {
          "psql",
          "-h",
          "localhost",
          "-p",
          String.valueOf(pgServer.getLocalPort()),
          "-c",
          "COPY all_types TO STDOUT (FORMAT BINARY)"
        };
    builder.command(psqlCommand);
    Process process = builder.start();
    String errors;

    try (BufferedReader errorReader =
        new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      errors = errorReader.lines().collect(Collectors.joining("\n"));
    }
    byte[] copyOutput = ByteStreams.toByteArray(process.getInputStream());

    assertEquals("", errors);
    int res = process.waitFor();
    assertEquals(0, res);

    ByteArrayOutputStream byteArrayOutputStream =
        new ByteArrayOutputStream(CopyToStatement.COPY_BINARY_HEADER.length);
    DataOutputStream expectedOutputStream = new DataOutputStream(byteArrayOutputStream);
    expectedOutputStream.write(CopyToStatement.COPY_BINARY_HEADER);
    expectedOutputStream.writeInt(0); // flags
    expectedOutputStream.writeInt(0); // header extension area length
    expectedOutputStream.writeShort(-1); // Column count == -1 means end of data.

    assertArrayEquals(byteArrayOutputStream.toByteArray(), copyOutput);
  }
}
