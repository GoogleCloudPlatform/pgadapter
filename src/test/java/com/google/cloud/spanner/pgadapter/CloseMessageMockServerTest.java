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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.wireprotocol.StartupMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import io.opentelemetry.api.OpenTelemetry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloseMessageMockServerTest extends AbstractMockServerTest {

  private static final Statement SELECT_TWO_ROWS = Statement.of("SELECT * FROM two_rows");
  private static final ResultSet TWO_ROWS_RESULTSET =
      ResultSet.newBuilder()
          .addAllRows(
              ImmutableList.of(
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("1").build())
                      .build(),
                  ListValue.newBuilder()
                      .addValues(Value.newBuilder().setStringValue("2").build())
                      .build()))
          .setMetadata(SELECT1_RESULTSET.getMetadata())
          .build();

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    doStartMockSpannerAndPgAdapterServers(
        createMockSpannerThatReturnsOneQueryPartition(),
        "d",
        configurator -> {},
        OpenTelemetry.noop());
  }

  @Before
  public void registerStatements() {
    mockSpanner.putStatementResult(StatementResult.query(SELECT1, SELECT1_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT2, SELECT2_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT_TWO_ROWS, TWO_ROWS_RESULTSET));
  }

  private void initializeConnection(DataInputStream inputStream, DataOutputStream outputStream)
      throws IOException {
    outputStream.writeInt(17);
    outputStream.writeInt(StartupMessage.PROTOCOL_VERSION_3_0_IDENTIFIER);
    outputStream.writeBytes("user");
    outputStream.writeByte(0);
    outputStream.writeBytes("foo");
    outputStream.writeByte(0);
    outputStream.flush();

    // AuthenticationOk ('R')
    assertEquals('R', inputStream.readByte());
    assertEquals(8, inputStream.readInt());
    assertEquals(0, inputStream.readInt());

    // BackendKeyData ('K')
    assertEquals('K', inputStream.readByte());
    assertEquals(12, inputStream.readInt());
    inputStream.readInt();
    inputStream.readInt();

    // Skip parameters until ReadyForQuery ('Z')
    skipUntilReadyForQuery(inputStream);
  }

  private void skipUntilReadyForQuery(DataInputStream inputStream) throws IOException {
    while (true) {
      byte messageType = inputStream.readByte();
      int length = inputStream.readInt();
      inputStream.readFully(new byte[length - 4]);
      if (messageType == 'Z') {
        break;
      }
    }
  }

  private void sendParse(DataOutputStream outputStream, String statementName, String sql)
      throws IOException {
    byte[] nameBytes = statementName.getBytes(StandardCharsets.UTF_8);
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
    outputStream.writeByte('P');
    outputStream.writeInt(4 + nameBytes.length + 1 + sqlBytes.length + 1 + 2);
    outputStream.write(nameBytes);
    outputStream.writeByte(0);
    outputStream.write(sqlBytes);
    outputStream.writeByte(0);
    outputStream.writeShort(0);
  }

  private void sendBind(DataOutputStream outputStream, String portalName, String statementName)
      throws IOException {
    byte[] portalBytes = portalName.getBytes(StandardCharsets.UTF_8);
    byte[] statementBytes = statementName.getBytes(StandardCharsets.UTF_8);
    outputStream.writeByte('B');
    outputStream.writeInt(4 + portalBytes.length + 1 + statementBytes.length + 1 + 2 + 2 + 2);
    outputStream.write(portalBytes);
    outputStream.writeByte(0);
    outputStream.write(statementBytes);
    outputStream.writeByte(0);
    outputStream.writeShort(0);
    outputStream.writeShort(0);
    outputStream.writeShort(0);
  }

  private void sendExecute(DataOutputStream outputStream, String portalName, int maxRows)
      throws IOException {
    byte[] portalBytes = portalName.getBytes(StandardCharsets.UTF_8);
    outputStream.writeByte('E');
    outputStream.writeInt(4 + portalBytes.length + 1 + 4);
    outputStream.write(portalBytes);
    outputStream.writeByte(0);
    outputStream.writeInt(maxRows);
  }

  private void sendDescribe(DataOutputStream outputStream, char type, String name)
      throws IOException {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    outputStream.writeByte('D');
    outputStream.writeInt(4 + 1 + nameBytes.length + 1);
    outputStream.writeByte(type);
    outputStream.write(nameBytes);
    outputStream.writeByte(0);
  }

  private void sendClose(DataOutputStream outputStream, char type, String name) throws IOException {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    outputStream.writeByte('C');
    outputStream.writeInt(4 + 1 + nameBytes.length + 1);
    outputStream.writeByte(type);
    outputStream.write(nameBytes);
    outputStream.writeByte(0);
  }

  private void sendSync(DataOutputStream outputStream) throws IOException {
    outputStream.writeByte('S');
    outputStream.writeInt(4);
  }

  private void sendFlush(DataOutputStream outputStream) throws IOException {
    outputStream.writeByte('H');
    outputStream.writeInt(4);
  }

  private void assertParseComplete(DataInputStream inputStream) throws IOException {
    assertEquals('1', inputStream.readByte());
    assertEquals(4, inputStream.readInt());
  }

  private void assertBindComplete(DataInputStream inputStream) throws IOException {
    assertEquals('2', inputStream.readByte());
    assertEquals(4, inputStream.readInt());
  }

  private void assertCloseComplete(DataInputStream inputStream) throws IOException {
    assertEquals('3', inputStream.readByte());
    assertEquals(4, inputStream.readInt());
  }

  private void assertCommandComplete(DataInputStream inputStream) throws IOException {
    assertEquals('C', inputStream.readByte());
    int length = inputStream.readInt();
    inputStream.readFully(new byte[length - 4]);
  }

  private void assertDataRow(DataInputStream inputStream) throws IOException {
    assertEquals('D', inputStream.readByte());
    int length = inputStream.readInt();
    inputStream.readFully(new byte[length - 4]);
  }

  private void assertDataRow(DataInputStream inputStream, String expectedValue) throws IOException {
    assertEquals('D', inputStream.readByte());
    int length = inputStream.readInt();
    short numColumns = inputStream.readShort();
    assertEquals(1, numColumns);
    int colLength = inputStream.readInt();
    byte[] colData = new byte[colLength];
    inputStream.readFully(colData);
    assertEquals(expectedValue, new String(colData, StandardCharsets.UTF_8));
  }

  private void assertPortalSuspended(DataInputStream inputStream) throws IOException {
    assertEquals('s', inputStream.readByte());
    assertEquals(4, inputStream.readInt());
  }

  private void assertParameterDescription(DataInputStream inputStream) throws IOException {
    assertEquals('t', inputStream.readByte());
    int length = inputStream.readInt();
    inputStream.readFully(new byte[length - 4]);
  }

  private void assertRowDescription(DataInputStream inputStream) throws IOException {
    assertEquals('T', inputStream.readByte());
    int length = inputStream.readInt();
    inputStream.readFully(new byte[length - 4]);
  }

  private void assertReadyForQuery(DataInputStream inputStream) throws IOException {
    assertReadyForQuery(inputStream, 'I');
  }

  private void assertReadyForQuery(DataInputStream inputStream, char status) throws IOException {
    assertEquals('Z', inputStream.readByte());
    assertEquals(5, inputStream.readInt());
    assertEquals(status, (char) inputStream.readByte());
  }

  private void assertErrorResponse(DataInputStream inputStream) throws IOException {
    assertEquals('E', inputStream.readByte());
    int length = inputStream.readInt();
    inputStream.readFully(new byte[length - 4]);
  }

  @Test
  public void testParseCloseSyncOrder() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Send pipeline: Parse(S1) -> Close(S0) -> Sync
      sendParse(outputStream, "S1", "SELECT 2");
      sendClose(outputStream, 'S', "S0");
      sendSync(outputStream);
      outputStream.flush();

      // Responses must arrive in exact request order:
      // ParseComplete ('1') -> CloseComplete ('3') -> ReadyForQuery ('Z')
      assertParseComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testExecuteCloseSyncOrder() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Send pipeline: Parse(S1) -> Bind(P1, S1) -> Execute(P1) -> Close(P1) -> Sync
      sendParse(outputStream, "S1", "SELECT 1");
      sendBind(outputStream, "P1", "S1");
      sendExecute(outputStream, "P1", 0);
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      // Responses must arrive in exact request order:
      // ParseComplete ('1') -> BindComplete ('2') -> DataRow ('D') -> CommandComplete ('C') ->
      // CloseComplete ('3') -> ReadyForQuery ('Z')
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertDataRow(inputStream);
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseThenParseSameNameInPipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Close(S0) -> Parse(S0, "SELECT 2") -> Sync
      sendClose(outputStream, 'S', "S0");
      sendParse(outputStream, "S0", "SELECT 2");
      sendSync(outputStream);
      outputStream.flush();

      // Responses must arrive in exact request order:
      // CloseComplete ('3') -> ParseComplete ('1') -> ReadyForQuery ('Z')
      assertCloseComplete(inputStream);
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Verify that S0 was re-parsed as "SELECT 2" and can be executed.
      sendBind(outputStream, "P0", "S0");
      sendExecute(outputStream, "P0", 0);
      sendClose(outputStream, 'P', "P0");
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertDataRow(inputStream, "2");
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseThenBindSameNameInPipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0: SELECT 1
      sendParse(outputStream, "S0", "SELECT 1");
      // Pre-parse S1: SELECT 2
      sendParse(outputStream, "S1", "SELECT 2");
      // Pre-bind P0 to S0
      sendBind(outputStream, "P0", "S0");
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Close(P0) -> Bind(P0, S1) -> Execute(P0) -> Sync
      sendClose(outputStream, 'P', "P0");
      sendBind(outputStream, "P0", "S1");
      sendExecute(outputStream, "P0", 0);
      sendSync(outputStream);
      outputStream.flush();

      // Responses must arrive in exact request order:
      // CloseComplete ('3') -> BindComplete ('2') -> DataRow ('D') -> CommandComplete ('C') ->
      // ReadyForQuery ('Z')
      assertCloseComplete(inputStream);
      assertBindComplete(inputStream);
      assertDataRow(inputStream, "2");
      assertCommandComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorSkipsCloseAndRestoresStatement() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline with error: Parse invalid SQL -> Close(S0) -> Sync
      sendParse(outputStream, "S1", "copy bad_syntax");
      sendClose(outputStream, 'S', "S0");
      sendSync(outputStream);
      outputStream.flush();

      // Response: ErrorResponse ('E') -> ReadyForQuery ('Z')
      // CloseComplete must NOT be emitted because the preceding statement failed
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // S0 must still be valid and executable because Close was aborted
      sendBind(outputStream, "P0", "S0");
      sendExecute(outputStream, "P0", 0);
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertDataRow(inputStream);
      assertCommandComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseNonExistentStatementAndPortal() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Close non-existent statement and portal
      sendClose(outputStream, 'S', "nonexistent_stmt");
      sendClose(outputStream, 'P', "nonexistent_portal");
      sendSync(outputStream);
      outputStream.flush();

      // Both should return CloseComplete
      assertCloseComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseWithFlush() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      sendClose(outputStream, 'S', "some_stmt");
      sendFlush(outputStream);
      outputStream.flush();

      // Flush returns CloseComplete without ReadyForQuery
      assertCloseComplete(inputStream);

      // Following with Sync emits ReadyForQuery
      sendSync(outputStream);
      outputStream.flush();
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testExecuteAndCloseSuspendedPortalInSamePipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Parse and bind P1 for a multi-row query, then execute with maxRows=1
      sendParse(outputStream, "S1", "SELECT * FROM two_rows");
      sendBind(outputStream, "P1", "S1");
      sendExecute(outputStream, "P1", 1);
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertDataRow(inputStream);
      assertPortalSuspended(inputStream);
      assertReadyForQuery(inputStream);

      // Next pipeline: Execute(P1, maxRows=1) -> Close(P1) -> Sync
      // Execute must stream remaining row before Close completes
      sendExecute(outputStream, "P1", 1);
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      assertDataRow(inputStream);
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Verify portal was actually closed
      sendExecute(outputStream, "P1", 1);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorWithActivePortalDropsPortal() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Prepare and bind P1, execute 1 row
      sendParse(outputStream, "S1", "SELECT * FROM two_rows");
      sendBind(outputStream, "P1", "S1");
      sendExecute(outputStream, "P1", 1);
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertDataRow(inputStream);
      assertPortalSuspended(inputStream);
      assertReadyForQuery(inputStream);

      // Send pipeline with an error before Close: Parse(bad) -> Close(P1) -> Sync
      sendParse(outputStream, "S_bad", "copy bad_syntax");
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      // ErrorResponse emitted, Close is aborted (no CloseComplete)
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // Verify portal P1 was dropped on error and cannot be executed
      sendExecute(outputStream, "P1", 1);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testFailedParseFollowedByCloseDoesNotResurrectStatement() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pipeline: Parse(bad) -> Close(S1) -> Sync
      sendParse(outputStream, "S1", "copy bad_syntax");
      sendClose(outputStream, 'S', "S1");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // Next query: Parse S1 with valid SQL must succeed without "Must close statement"
      sendParse(outputStream, "S1", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseUnnamedStatementAndPortal() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Parse and bind unnamed statement and portal, then close both
      sendParse(outputStream, "", "SELECT 1");
      sendBind(outputStream, "", "");
      sendClose(outputStream, 'P', "");
      sendClose(outputStream, 'S', "");
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertCloseComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testCloseFollowedByBindInSamePipelineFails() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Close(S0) -> Bind(P0, S0) -> Sync
      sendClose(outputStream, 'S', "S0");
      sendBind(outputStream, "P0", "S0");
      sendSync(outputStream);
      outputStream.flush();

      // Close completes, then Bind fails with prepared statement does not exist
      assertCloseComplete(inputStream);
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorSkipsCloseAndRestoresStatementWithSubsequentParse()
      throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0: SELECT 1
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline with an error before Close(S0), followed by Parse(S0) reusing the same name:
      // Pipeline: Parse(bad) -> Close(S0) -> Parse(S0, SELECT 2) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendClose(outputStream, 'S', "S0");
      sendParse(outputStream, "S0", "SELECT 2");
      sendSync(outputStream);
      outputStream.flush();

      // Only the error response and ready for query are received
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // Verify that S0 was restored to its original definition ("SELECT 1") and was not lost
      // Bind P0 to S0 and execute
      sendBind(outputStream, "P0", "S0");
      sendExecute(outputStream, "P0", 0);
      sendClose(outputStream, 'P', "P0");
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertDataRow(inputStream, "1");
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorWithCloseDropsPortalWithSubsequentBind() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0: SELECT 1
      sendParse(outputStream, "S0", "SELECT 1");
      // Pre-bind P0 to S0
      sendBind(outputStream, "P0", "S0");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pre-parse S1: SELECT 2
      sendParse(outputStream, "S1", "SELECT 2");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline with an error before Close(P0), followed by Bind(P0, S1) reusing the same portal
      // name:
      // Pipeline: Parse(bad) -> Close(P0) -> Bind(P0, S1) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendClose(outputStream, 'P', "P0");
      sendBind(outputStream, "P0", "S1");
      sendSync(outputStream);
      outputStream.flush();

      // Only the error response and ready for query are received
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // When the pipeline failed, closeAllPortals closed all portals. P0 must NOT exist.
      sendExecute(outputStream, "P0", 0);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorSkipsCloseAndRestoresStatementCreatedInSamePipeline()
      throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pipeline: Parse(S1) -> Parse(bad) -> Close(S1) -> Sync
      sendParse(outputStream, "S1", "SELECT 1");
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendClose(outputStream, 'S', "S1");
      sendSync(outputStream);
      outputStream.flush();

      assertParseComplete(inputStream);
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // S1 must still exist because Parse(S1) succeeded and Close(S1) was skipped/aborted!
      sendBind(outputStream, "P1", "S1");
      sendExecute(outputStream, "P1", 0);
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertDataRow(inputStream, "1");
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorWithCloseDropsPortalCreatedInSamePipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S1: SELECT 1
      sendParse(outputStream, "S1", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Bind(P1, S1) -> Parse(bad) -> Close(P1) -> Sync
      sendBind(outputStream, "P1", "S1");
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // P1 was dropped when the pipeline failed and must NOT exist.
      sendExecute(outputStream, "P1", 0);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void
      testPipelineErrorWithDescribeAndCloseDoesNotThrowIllegalStateExceptionAndRestoresStatement()
          throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0: SELECT 1
      sendParse(outputStream, "S0", "SELECT 1");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Parse(bad) -> Describe('S', S0) -> Close('S', S0) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendDescribe(outputStream, 'S', "S0");
      sendClose(outputStream, 'S', "S0");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // S0 should be restored and usable (no IllegalStateException from uncompleted future)
      sendDescribe(outputStream, 'S', "S0");
      sendSync(outputStream);
      outputStream.flush();

      assertParameterDescription(inputStream);
      assertRowDescription(inputStream);
      assertReadyForQuery(inputStream);

      sendBind(outputStream, "P0", "S0");
      sendExecute(outputStream, "P0", 0);
      sendClose(outputStream, 'P', "P0");
      sendSync(outputStream);
      outputStream.flush();

      assertBindComplete(inputStream);
      assertDataRow(inputStream, "1");
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testPipelineErrorWithExecuteAndCloseDoesNotThrowIllegalStateExceptionAndDropsPortal()
      throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pre-parse S0: SELECT 1
      sendParse(outputStream, "S0", "SELECT 1");
      sendBind(outputStream, "P1", "S0");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertReadyForQuery(inputStream);

      // Pipeline: Parse(bad) -> Execute(P1, 1) -> Close('P', P1) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendExecute(outputStream, "P1", 1);
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // P1 was dropped on error and cannot be executed
      sendExecute(outputStream, "P1", 0);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }

  @Test
  public void testTransactionAbortDropsPortalEvenIfClosedInFailingPipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Start an explicit transaction
      sendParse(outputStream, "begin", "BEGIN");
      sendBind(outputStream, "p_begin", "begin");
      sendExecute(outputStream, "p_begin", 0);
      sendClose(outputStream, 'P', "p_begin");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream, 'T');

      // Create portal P1 in the transaction
      sendParse(outputStream, "S0", "SELECT 1");
      sendBind(outputStream, "P1", "S0");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertReadyForQuery(inputStream, 'T');

      // Pipeline that fails: Parse(bad) -> Close('P', P1) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendClose(outputStream, 'P', "P1");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      // Transaction is now in ABORTED state ('E')
      assertReadyForQuery(inputStream, 'E');

      // Rollback the transaction
      sendParse(outputStream, "rollback", "ROLLBACK");
      sendBind(outputStream, "p_rollback", "rollback");
      sendExecute(outputStream, "p_rollback", 0);
      sendClose(outputStream, 'P', "p_rollback");
      sendSync(outputStream);
      outputStream.flush();
      assertParseComplete(inputStream);
      assertBindComplete(inputStream);
      assertCommandComplete(inputStream);
      assertCloseComplete(inputStream);
      assertReadyForQuery(inputStream, 'I');

      // P1 must NOT exist after the aborted transaction
      sendExecute(outputStream, "P1", 0);
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream, 'I');
    }
  }

  @Test
  public void testPipelineErrorAbortsBothCreationAndCloseInSamePipeline() throws Exception {
    try (Socket socket = new Socket("localhost", pgServer.getLocalPort());
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
      initializeConnection(inputStream, outputStream);

      // Pipeline: Parse(bad) -> Parse(S1) -> Close(S1) -> Sync
      sendParse(outputStream, "bad", "copy bad_syntax");
      sendParse(outputStream, "S1", "SELECT 1");
      sendClose(outputStream, 'S', "S1");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);

      // S1 was created after the error, so both Parse(S1) and Close(S1) were aborted.
      // S1 must not exist.
      sendBind(outputStream, "P1", "S1");
      sendSync(outputStream);
      outputStream.flush();

      assertErrorResponse(inputStream);
      assertReadyForQuery(inputStream);
    }
  }
}
