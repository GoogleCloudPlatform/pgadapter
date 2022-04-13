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
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Database;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public final class ITQueryTest implements IntegrationTest {
  private static final PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
  private static Database database;

  @BeforeClass
  public static void setup() {
    database = testEnv.createDatabase(getDdlStatements());
    testEnv.startPGAdapterServer(database.getId(), Collections.emptyList());
  }

  @AfterClass
  public static void teardown() {
    testEnv.stopPGAdapterServer();
    testEnv.cleanUp();
  }

  private static Iterable<String> getDdlStatements() {
    return Collections.singletonList(
        "CREATE TABLE users (\n"
            + "  id     bigint PRIMARY KEY,\n"
            + "  age    bigint,\n"
            + "  name   text,\n"
            + "  data   numeric\n"
            + ");");
  }

  @Before
  public void insertTestData() {
    List<String> values =
        new ArrayList<>(
            Arrays.asList(
                "(1, 1, '1', '12345.67890')", "(2, 20, 'ABCD', 'NaN')", "(3, 23, 'Jack', '22')"));
    String dml = "INSERT INTO users (id, age, name, data) VALUES " + String.join(", ", values);
    testEnv.updateTables(
        database.getId().getDatabase(), ImmutableList.of("delete from users", dml));
  }

  @Test
  public void simplePgQuery() throws Exception {
    testEnv.waitForServer();

    Socket clientSocket = new Socket(testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
    DataInputStream in = new DataInputStream(clientSocket.getInputStream());
    testEnv.initializeConnection(out);
    testEnv.consumeStartUpMessages(in);

    // Run a query that is PG specific to ensure this is a PG dialect database.
    String payload = "SELECT 42::int8\0";
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) (payload.length() + 4)};
    byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
    out.write(message, 0, message.length);

    // Check for correct message type identifiers. Exactly 1 row should be returned.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    testEnv.consumePGMessage('T', in);
    PgAdapterTestEnv.PGMessage dataRow = testEnv.consumePGMessage('D', in);
    testEnv.consumePGMessage('C', in);
    testEnv.consumePGMessage('Z', in);

    // Check data returned payload
    // see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
    DataInputStream dataRowIn = new DataInputStream(new ByteArrayInputStream(dataRow.getPayload()));
    // Number of column values.
    assertEquals(1, dataRowIn.readShort());
    // Column value length (2 bytes expected)
    assertEquals(2, dataRowIn.readInt());
    // Value of the column: '42'
    assertEquals('4', dataRowIn.readByte());
    assertEquals('2', dataRowIn.readByte());
  }

  @Test
  public void basicSelectTest() throws Exception {
    testEnv.waitForServer();

    Socket clientSocket = new Socket(testEnv.getPGAdapterHost(), testEnv.getPGAdapterPort());
    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
    DataInputStream in = new DataInputStream(clientSocket.getInputStream());
    testEnv.initializeConnection(out);
    testEnv.consumeStartUpMessages(in);

    // Send query.
    String payload = "SELECT id, name, age, data FROM users\0";
    byte[] messageMetadata = {'Q', 0, 0, 0, (byte) (payload.length() + 4)};
    byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
    out.write(message, 0, message.length);

    // Check for correct message type identifiers.
    // See https://www.postgresql.org/docs/13/protocol-message-formats.html for more details.
    PgAdapterTestEnv.PGMessage rowDescription = testEnv.consumePGMessage('T', in);
    PgAdapterTestEnv.PGMessage[] dataRows = {
      testEnv.consumePGMessage('D', in),
      testEnv.consumePGMessage('D', in),
      testEnv.consumePGMessage('D', in)
    };
    PgAdapterTestEnv.PGMessage commandComplete = testEnv.consumePGMessage('C', in);
    PgAdapterTestEnv.PGMessage readyForQuery = testEnv.consumePGMessage('Z', in);

    // Check RowDescription payload
    // see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
    DataInputStream rowDescIn =
        new DataInputStream(new ByteArrayInputStream(rowDescription.getPayload()));
    short fieldCount = rowDescIn.readShort();
    assertEquals(4, fieldCount);
    for (String expectedFieldName : new String[] {"id", "name", "age", "data"}) {
      // Read a null-terminated string.
      StringBuilder builder = new StringBuilder();
      byte b;
      while ((b = rowDescIn.readByte()) != (byte) 0) {
        builder.append((char) b);
      }
      String fieldName = builder.toString();
      assertEquals(expectedFieldName, fieldName);
      byte[] unusedBytes = new byte[18];
      rowDescIn.readFully(unusedBytes);
    }

    // Check exact message results.
    byte[] rowDescriptionData = {
      0, 4, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8, 0, 0, 0, 0, 0, 0, 110, 97, 109, 101,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 19, -1, -1, 0, 0, 0, 0, 0, 0, 97, 103, 101, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 20, 0, 8, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, -92, -1,
      -1, 0, 0, 0, 0, 0, 0
    };
    byte[] dataRow0 = {
      0, 4, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 11, 49, 50, 51, 52, 53, 46, 54,
      55, 56, 57, 48
    };
    byte[] dataRow1 = {
      0, 4, 0, 0, 0, 1, 50, 0, 0, 0, 4, 65, 66, 67, 68, 0, 0, 0, 2, 50, 48, 0, 0, 0, 3, 78, 97, 78
    };
    byte[] dataRow2 = {
      0, 4, 0, 0, 0, 1, 51, 0, 0, 0, 4, 74, 97, 99, 107, 0, 0, 0, 2, 50, 51, 0, 0, 0, 2, 50, 50
    };
    byte[] commandCompleteData = {83, 69, 76, 69, 67, 84, 32, 51, 0};
    byte[] readyForQueryData = {73};

    assertArrayEquals(rowDescriptionData, rowDescription.getPayload());
    assertArrayEquals(dataRow0, dataRows[0].getPayload());
    assertArrayEquals(dataRow1, dataRows[1].getPayload());
    assertArrayEquals(dataRow2, dataRows[2].getPayload());
    assertArrayEquals(commandCompleteData, commandComplete.getPayload());
    assertArrayEquals(readyForQueryData, readyForQuery.getPayload());
  }
}
