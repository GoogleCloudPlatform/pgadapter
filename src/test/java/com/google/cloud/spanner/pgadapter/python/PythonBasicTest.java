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

package com.google.cloud.spanner.pgadapter.python;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(PythonTest.class)
public class PythonBasicTest extends AbstractMockServerTest {
  static String execute(int port, String test) throws IOException, InterruptedException {
    String[] runCommand =
        new String[] {"python3", "pgAdapterBasicTests.py", Integer.toString(port), test};
    ProcessBuilder builder = new ProcessBuilder();
    builder.command(runCommand);
    builder.directory(new File("./src/test/python"));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());

    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    /*try {
      assertEquals(output.toString(), 0, result);
    }catch (Exception e){
      throw SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT,
          output.toString());
    }
*/
    if(result != 0)
      throw SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT,
          output.toString());

    return output.toString();
  }

  private ResultSet createResultset() {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("Id")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("Name")
                            .build())
                    .build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue(String.valueOf(1)).build())
            .addValues(Value.newBuilder().setStringValue("abcd").build())
            .build());
    ResultSet resultSet = resultSetBuilder.build();
    return resultSet;
  }

  @Test
  public void testBasic() throws IOException, InterruptedException {
    String sql = "SELECT * FROM some_table";

    mockSpanner.putStatementResult(StatementResult.query(Statement.of(sql), createResultset()));

    String actualOutput = execute(pgServer.getLocalPort(), sql);
    String expectedOutput = "(1, \'abcd\')\n";
    assertEquals(expectedOutput, actualOutput);
  }
}
