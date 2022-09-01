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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.IOException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PythonTest.class)
public class PythonCopyTests extends PythonTestSetup {

  private static ResultSet createResultSet() {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("column_name")
                            .build())
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .setName("data_type")
                            .build())
                    .build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("id").build())
            .addValues(Value.newBuilder().setStringValue("bigint").build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("name").build())
            .addValues(Value.newBuilder().setStringValue("character varying").build())
            .build());
    return resultSetBuilder.build();
  }

  private static ResultSet createResultSetForIndexColumns() {
    ResultSet.Builder resultSetBuilder = ResultSet.newBuilder();

    resultSetBuilder.setMetadata(
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .setName("COUNT")
                            .build())
                    .build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("0").build()).build());
    return resultSetBuilder.build();
  }

  private static ResultSet createResultSetForSelect() {
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
            .addValues(Value.newBuilder().setStringValue("1").build())
            .addValues(Value.newBuilder().setStringValue("hello").build())
            .build());
    resultSetBuilder.addRows(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("2").build())
            .addValues(Value.newBuilder().setStringValue("world").build())
            .build());
    return resultSetBuilder.build();
  }

  @Test
  public void copyFromTest() throws IOException, InterruptedException {
    String sql = "COPY test from STDIN CSV DELIMITER ','";
    String copyType = "FROM";
    String file = "1,hello\n2,world\n";
    String sql1 =
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1";
    String sql2 =
        "SELECT COUNT(*) FROM information_schema.index_columns WHERE table_schema='public' and table_name=$1 and column_name in ($2, $3)";
    Statement s1 = Statement.newBuilder(sql1).bind("p1").to("test").build();

    Statement s2 =
        Statement.newBuilder(sql2)
            .bind("p1")
            .to("test")
            .bind("p2")
            .to("id")
            .bind("p3")
            .to("name")
            .build();
    mockSpanner.putStatementResult(StatementResult.query(s1, createResultSet()));
    mockSpanner.putStatementResult(StatementResult.query(s2, createResultSetForIndexColumns()));
    String actualOutput = executeCopy(pgServer.getLocalPort(), sql, file, copyType);

    assertEquals("2\n", actualOutput);
  }

  @Test
  public void copyToTest() throws IOException, InterruptedException {
    String sql = "COPY test TO STDOUT DELIMITER ','";
    String copyType = "TO";
    String file = "does not matter";
    String sql1 =
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1";
    Statement s1 = Statement.newBuilder(sql1).bind("p1").to("test").build();
    mockSpanner.putStatementResult(StatementResult.query(s1, createResultSet()));

    String sql2 =
        "SELECT COUNT(*) FROM information_schema.index_columns WHERE table_schema='public' and table_name=$1 and column_name in ($2, $3)";
    Statement s2 =
        Statement.newBuilder(sql2)
            .bind("p1")
            .to("test")
            .bind("p2")
            .to("id")
            .bind("p3")
            .to("name")
            .build();
    mockSpanner.putStatementResult(StatementResult.query(s2, createResultSetForIndexColumns()));

    String sql3 = "select * from test";
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of(sql3), createResultSetForSelect()));
    String actualOutput = executeCopy(pgServer.getLocalPort(), sql, file, copyType);
    String expectedOutput = "1,hello\n" + "2,world\n";

    assertEquals(expectedOutput, actualOutput);
  }
}
