// Copyright 2021 Google LLC
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

import com.google.cloud.spanner.Database;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public final class ITParameterizedQueryTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  private Socket clientSocket;
  private DataOutputStream out;
  private DataInputStream in;

  @BeforeClass
  public static void setup() {
    testEnv.setUp();
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServer(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  @Before
  public void insertTestData() {
    List<String> values =
        new ArrayList<>(Arrays.asList("(1, 1, '1')", "(2, 20, 'Joe')", "(3, 23, 'Jack')"));
    String dml = "INSERT INTO users (id, age, name) VALUES " + String.join(", ", values);
    testEnv.updateTables(
        database.getId().getDatabase(), ImmutableList.of("delete from users", dml));
  }

  private static Iterable<String> getDdlStatements() {
    return Collections.singletonList(
        "CREATE TABLE users (\n"
            + "  id   bigint PRIMARY KEY,\n"
            + "  age  bigint,\n"
            + "  name   text\n"
            + ");");
  }

  @After
  public void closeSocket() throws Exception {
    clientSocket.close();
    out.close();
    in.close();
  }

  // '\0' for name will refer to an unnamed statement.
  private byte[] constructParseMessage(
      short parameters, String query, String name, ArrayList<Integer> oidTypes) {
    // Unnamed prepared statement = "\0"
    ByteBuffer metadata = ByteBuffer.allocate(5);
    metadata.put(0, (byte) 'P');
    // Message size: 4 additional bytes per parameter + 4 for size + 2 for number of parameters
    metadata.putInt(1, name.length() + query.length() + parameters * 4 + 6);

    ByteBuffer typeBuffer = ByteBuffer.allocate(parameters * 4 + 2);
    typeBuffer.putShort(parameters);
    for (Integer oidType : oidTypes) {
      typeBuffer.putInt(oidType);
    }
    return Bytes.concat(metadata.array(), name.getBytes(), query.getBytes(), typeBuffer.array());
  }

  // '\0' for statementName or portalName will refer to an unnamed statement.
  private byte[] constructBindMessage(
      String statementName, String portalName, ArrayList<PgAdapterTestEnv.Parameter> values) {
    // 5 bytes: 1 byte for message type + 4 for size
    ByteBuffer metadata = ByteBuffer.allocate(5);
    metadata.put(0, (byte) 'B');
    int valueSize = 0;
    for (PgAdapterTestEnv.Parameter value : values) {
      // 4 bytes for size field + parameter value size
      valueSize += value.getSize() + 4;
    }
    // Message size:  4 bytes for size +
    //                total size of parameters (includes parameter sizes) +
    //                2 bytes per parameter for parameter length +
    //                2 bytes for number of parameters +
    //                2 bytes for number of parameter format codes +
    //                2 bytes indicating number of result column format feilds (0 == use default on
    // all)
    metadata.putInt(
        1, portalName.length() + statementName.length() + valueSize + values.size() * 2 + 10);

    // 2 bytes for number of format codes + 2 bytes per format code + 2 bytes for number of
    // parameters
    ByteBuffer parameterData = ByteBuffer.allocate(values.size() * 2 + 4);
    parameterData.putShort((short) values.size());
    for (int i = 0; i < values.size(); ++i) {
      // 0 = TEXT, 1 = BINARY
      parameterData.putShort((short) 0);
    }
    // Number of parameters
    parameterData.putShort((short) values.size());

    // 4 bytes per parameter for size + size of paramter itself
    byte[] parameterValues = {};
    for (PgAdapterTestEnv.Parameter value : values) {
      byte[] sizeArray = ByteBuffer.allocate(4).putInt(value.getSize()).array();
      parameterValues = Bytes.concat(parameterValues, sizeArray, value.getValue());
    }
    // 2 bytes for number of result column format codes (0 == use default for all)
    ByteBuffer trailingMetadata = ByteBuffer.allocate(2);
    trailingMetadata.putShort((short) 0);

    return Bytes.concat(
        metadata.array(),
        portalName.getBytes(),
        statementName.getBytes(),
        parameterData.array(),
        parameterValues,
        trailingMetadata.array());
  }

  // Constructs a Describe message that will describe either a portal (portal == true) or a prepared
  // statement (portal == false)
  private byte[] constructDescribeMessage(boolean portal, String name) {
    // 6 bytes: 1 byte for message type + 4 for size + 1 for Portal/Statement type
    ByteBuffer metadata = ByteBuffer.allocate(6);
    metadata.put(0, (byte) 'D');
    metadata.putInt(1, name.length() + 5);
    if (portal) {
      // Portal
      metadata.put(5, (byte) 'P');
    } else {
      // Statement
      metadata.put(5, (byte) 'S');
    }
    return Bytes.concat(metadata.array(), name.getBytes());
  }

  private byte[] constructExecuteMessage(String portalName) {
    // 5 bytes: 1 byte for message type + 4 for size
    ByteBuffer metadata = ByteBuffer.allocate(5);
    metadata.put(0, (byte) 'E');
    // 8 bytes: 4 bytes for size (includes self) + 4 for maximum number of return rows (0 == no
    // limit)
    metadata.putInt(1, portalName.length() + 8);
    ByteBuffer limit = ByteBuffer.allocate(4);
    limit.putInt(0, 0);
    return Bytes.concat(metadata.array(), portalName.getBytes(), limit.array());
  }

  // If portal is true this will close the corresponding portal, if false this will close the
  // corresponding prepared statement.
  private byte[] constructCloseMessage(boolean portal, String name) {
    // 6 bytes: 1 byte for message type + 4 for size + 1 for Portal/Statement type
    ByteBuffer metadata = ByteBuffer.allocate(6);
    metadata.put(0, (byte) 'C');
    metadata.putInt(1, name.length() + 5);
    if (portal) {
      // Portal
      metadata.put(5, (byte) 'P');
    } else {
      // Statement
      metadata.put(5, (byte) 'S');
    }
    return Bytes.concat(metadata.array(), name.getBytes());
  }

  private byte[] constructSyncMessage() {
    // 5 bytes: 1 byte for message type + 4 for size
    ByteBuffer metadata = ByteBuffer.allocate(5);
    metadata.put(0, (byte) 'S');
    metadata.putInt(1, 4);
    return metadata.array();
  }

  @Test
  public void unnamedParameterTest() throws Exception {
    testEnv.waitForServer();

    clientSocket = new Socket(testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    out = new DataOutputStream(clientSocket.getOutputStream());
    in = new DataInputStream(clientSocket.getInputStream());
    testEnv.initializeConnection(out);
    testEnv.consumeStartUpMessages(in);

    // Send Parse message.
    ArrayList<Integer> oidList = new ArrayList<>();
    oidList.add(Oid.INT8);
    byte[] parseMessage =
        constructParseMessage((short) 1, "SELECT * FROM users WHERE id = $1\0", "\0", oidList);
    out.write(parseMessage, 0, parseMessage.length);
    // Check for parse complete message.
    testEnv.consumePGMessage('1', in);

    // Send Bind message.
    ArrayList<PgAdapterTestEnv.Parameter> paramList = new ArrayList<>();
    String param1 = "1";
    paramList.add(new PgAdapterTestEnv.Parameter(Oid.INT8, param1));
    byte[] bindMessage = constructBindMessage("\0", "\0", paramList);
    out.write(bindMessage, 0, bindMessage.length);
    // Check for bind complete message.
    testEnv.consumePGMessage('2', in);

    // Send Execute message.
    byte[] exectueMessage = constructExecuteMessage("\0");
    out.write(exectueMessage, 0, exectueMessage.length);
    PgAdapterTestEnv.PGMessage[] dataRows = {testEnv.consumePGMessage('D', in)};
    PgAdapterTestEnv.PGMessage commandComplete = testEnv.consumePGMessage('C', in);

    // Send Close message.
    byte[] closeMessage = constructCloseMessage(true, "\0");
    out.write(closeMessage, 0, closeMessage.length);
    testEnv.consumePGMessage('3', in);

    // Send Sync message.
    byte[] syncMessage = constructSyncMessage();
    out.write(syncMessage, 0, syncMessage.length);
    PgAdapterTestEnv.PGMessage readyForQuery = testEnv.consumePGMessage('Z', in);

    // Verify results
    byte[] dataRow0 = {0, 3, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49};
    byte[] commandCompleteData = {83, 69, 76, 69, 67, 84, 32, 49, 0};
    byte[] readyForQueryData = {73};

    assertArrayEquals(dataRow0, dataRows[0].getPayload());
    assertArrayEquals(commandCompleteData, commandComplete.getPayload());
    assertArrayEquals(readyForQueryData, readyForQuery.getPayload());
  }

  @Test
  public void parameterTest() throws Exception {
    testEnv.waitForServer();
    String statementName = "test-statement\0";
    String portalName = "test-portal\0";

    clientSocket = new Socket(testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    out = new DataOutputStream(clientSocket.getOutputStream());
    in = new DataInputStream(clientSocket.getInputStream());
    testEnv.initializeConnection(out);
    testEnv.consumeStartUpMessages(in);

    // Send Parse message.
    ArrayList<Integer> oidList = new ArrayList<>();
    oidList.add(Oid.INT8);
    byte[] parseMessage =
        constructParseMessage(
            (short) 1, "SELECT * FROM users WHERE id = $1\0", statementName, oidList);
    out.write(parseMessage, 0, parseMessage.length);
    // Check for parse complete message.
    testEnv.consumePGMessage('1', in);

    // Send Bind message.
    ArrayList<PgAdapterTestEnv.Parameter> paramList = new ArrayList<>();
    String param1 = "1";
    paramList.add(new PgAdapterTestEnv.Parameter(Oid.INT8, param1));
    byte[] bindMessage = constructBindMessage(statementName, portalName, paramList);
    out.write(bindMessage, 0, bindMessage.length);
    // Check for bind complete message.
    testEnv.consumePGMessage('2', in);

    // Send Execute message.
    byte[] exectueMessage = constructExecuteMessage(portalName);
    out.write(exectueMessage, 0, exectueMessage.length);
    PgAdapterTestEnv.PGMessage[] dataRows = {testEnv.consumePGMessage('D', in)};
    PgAdapterTestEnv.PGMessage commandComplete = testEnv.consumePGMessage('C', in);

    // Send Close message.
    byte[] closeMessage = constructCloseMessage(true, portalName);
    out.write(closeMessage, 0, closeMessage.length);
    testEnv.consumePGMessage('3', in);

    // Send Sync message.
    byte[] syncMessage = constructSyncMessage();
    out.write(syncMessage, 0, syncMessage.length);
    PgAdapterTestEnv.PGMessage readyForQuery = testEnv.consumePGMessage('Z', in);

    // Verify results
    byte[] dataRow0 = {0, 3, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49};
    byte[] commandCompleteData = {83, 69, 76, 69, 67, 84, 32, 49, 0};
    byte[] readyForQueryData = {73};

    assertArrayEquals(dataRow0, dataRows[0].getPayload());
    assertArrayEquals(commandCompleteData, commandComplete.getPayload());
    assertArrayEquals(readyForQueryData, readyForQuery.getPayload());
  }
}
