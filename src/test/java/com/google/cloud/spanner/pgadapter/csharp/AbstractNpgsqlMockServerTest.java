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

package com.google.cloud.spanner.pgadapter.csharp;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.junit.BeforeClass;

public abstract class AbstractNpgsqlMockServerTest extends AbstractMockServerTest {
  private static final Statement SELECT_VERSION = Statement.of("SELECT version()");
  private static final ResultSetMetadata SELECT_VERSION_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("version")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_VERSION_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(
                      Value.newBuilder()
                          .setStringValue(
                              "PostgreSQL 13.4 on x86_64-apple-darwin, compiled by Apple clang version 11.0.3 (clang-1103.0.32.59), 64-bit")
                          .build())
                  .build())
          .setMetadata(SELECT_VERSION_METADATA)
          .build();
  private static final Statement SELECT_TYPES =
      Statement.of(
          "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid\n"
              + "FROM (\n"
              + "    \n"
              + "    \n"
              + "    \n"
              + "    SELECT\n"
              + "        typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,\n"
              + "        elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,\n"
              + "        CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype\n"
              + "    FROM (\n"
              + "        SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,\n"
              + "            CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,\n"
              + "            CASE\n"
              + "                WHEN proc.proname='array_recv' THEN typ.typelem\n"
              + "                \n"
              + "                \n"
              + "                WHEN typ.typtype='d' THEN typ.typbasetype\n"
              + "            END AS elemtypoid\n"
              + "        FROM pg_type AS typ\n"
              + "        LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
              + "        LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive\n"
              + "        \n"
              + "    ) AS typ\n"
              + "    LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid\n"
              + "    LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)\n"
              + "    LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive\n"
              + ") AS t\n"
              + "JOIN pg_namespace AS ns ON (ns.oid = typnamespace)\n"
              + "WHERE\n"
              + "    typtype IN ('b', 'r', 'm', 'e', 'd') OR \n"
              + "    (typtype = 'c' AND relkind='c') OR \n"
              + "    (typtype = 'p' AND typname IN ('record', 'void')) OR \n"
              + "    (typtype = 'a' AND (  \n"
              + "        elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR \n"
              + "        (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR \n"
              + "        (elemtyptype = 'c' AND elemrelkind='c') \n"
              + "    ))\n"
              + "ORDER BY CASE\n"
              + "       WHEN typtype IN ('b', 'e', 'p') THEN 0           \n"
              + "       WHEN typtype = 'r' THEN 1                        \n"
              + "       WHEN typtype = 'm' THEN 2                        \n"
              + "       WHEN typtype = 'c' THEN 3                        \n"
              + "       WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 \n"
              + "       WHEN typtype = 'a' THEN 5                        \n"
              + "       WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  \n"
              + "END");
  // ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
  private static final ResultSetMetadata SELECT_TYPES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("nspname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("oid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typtype")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("typnotnull")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("elemtypoid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_TYPES_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(SELECT_TYPES_METADATA).build();
  private static final Statement SELECT_ATTRIBUTES =
      Statement.of(
          "SELECT typ.oid, att.attname, att.atttypid\n"
              + "FROM pg_type AS typ\n"
              + "JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\n"
              + "JOIN pg_class AS cls ON (cls.oid = typ.typrelid)\n"
              + "JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\n"
              + "WHERE\n"
              + "  (typ.typtype = 'c' AND cls.relkind='c') AND\n"
              + "  attnum > 0 AND     \n"
              + "  NOT attisdropped\n"
              + "ORDER BY typ.oid, att.attnum");
  private static final ResultSetMetadata SELECT_ATTRIBUTES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("oid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("attname")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("atttypid")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT_ATTRIBUTES_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder().setMetadata(SELECT_ATTRIBUTES_METADATA).build();

  @BeforeClass
  public static void setupResults() {
    mockSpanner.putStatementResult(StatementResult.query(SELECT_VERSION, SELECT_VERSION_RESULTSET));
    mockSpanner.putStatementResult(StatementResult.query(SELECT_TYPES, SELECT_TYPES_RESULTSET));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_ATTRIBUTES, SELECT_ATTRIBUTES_RESULTSET));
  }

  static String execute(String test, String connectionString)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder();
    String[] runCommand = new String[] {"dotnet", "run", test, connectionString};
    builder.command(runCommand);
    builder.directory(new File("./src/test/csharp/pgadapter_npgsql_tests/npgsql_tests"));
    Process process = builder.start();
    Scanner scanner = new Scanner(process.getInputStream());
    StringBuilder output = new StringBuilder();
    while (scanner.hasNextLine()) {
      output.append(scanner.nextLine()).append("\n");
    }
    int result = process.waitFor();
    assertEquals(output.toString(), 0, result);

    return output.toString();
  }
}
