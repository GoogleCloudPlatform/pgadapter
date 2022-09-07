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

package com.google.cloud.spanner.pgadapter.statements;

import static com.google.cloud.spanner.pgadapter.statements.PrepareStatement.dataTypeNameToOid;
import static com.google.cloud.spanner.pgadapter.statements.PrepareStatement.parse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.statements.PrepareStatement.ParsedPreparedStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class PrepareStatementTest {

  @Test
  public void testDataTypeNameToOid() {
    assertEquals(Oid.INT8, dataTypeNameToOid("bigint"));
    assertEquals(Oid.INT8_ARRAY, dataTypeNameToOid("bigint[]"));
    assertEquals(Oid.INT8, dataTypeNameToOid("int8"));
    assertEquals(Oid.INT8_ARRAY, dataTypeNameToOid("int8[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("int"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("int[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("int4"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("int4[]"));
    assertEquals(Oid.INT4, dataTypeNameToOid("integer"));
    assertEquals(Oid.INT4_ARRAY, dataTypeNameToOid("integer[]"));
    assertEquals(Oid.BOOL, dataTypeNameToOid("boolean"));
    assertEquals(Oid.BOOL_ARRAY, dataTypeNameToOid("boolean[]"));
    assertEquals(Oid.BOOL, dataTypeNameToOid("bool"));
    assertEquals(Oid.BOOL_ARRAY, dataTypeNameToOid("bool[]"));
    assertEquals(Oid.DATE, dataTypeNameToOid("date"));
    assertEquals(Oid.DATE_ARRAY, dataTypeNameToOid("date[]"));
    assertEquals(Oid.FLOAT8, dataTypeNameToOid("double precision"));
    assertEquals(Oid.FLOAT8_ARRAY, dataTypeNameToOid("double precision[]"));
    assertEquals(Oid.FLOAT8, dataTypeNameToOid("float8"));
    assertEquals(Oid.FLOAT8_ARRAY, dataTypeNameToOid("float8[]"));
    assertEquals(Oid.JSONB, dataTypeNameToOid("jsonb"));
    assertEquals(Oid.JSONB_ARRAY, dataTypeNameToOid("jsonb[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("numeric"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("numeric[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("decimal"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("decimal[]"));
    assertEquals(Oid.NUMERIC, dataTypeNameToOid("numeric(1,1)"));
    assertEquals(Oid.NUMERIC_ARRAY, dataTypeNameToOid("numeric(2, 1)[]"));

    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying(100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar(100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character varying (100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar (100)"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("character     varying   ( 100 )  \t"));
    assertEquals(Oid.VARCHAR, dataTypeNameToOid("varchar\t(100)  \n"));

    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying(100)[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar(100)[]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character varying (100) []"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar (100) []"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("character     varying   ( 100 )  \t[\n]"));
    assertEquals(Oid.VARCHAR_ARRAY, dataTypeNameToOid("varchar\t(100)  \n[  ]"));
  }

  @Test
  public void testParse() {
    ParsedPreparedStatement statement = parse("prepare foo as select 1");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {}, statement.dataTypes);
    assertEquals("select 1", statement.originalPreparedStatement.getSql());

    statement = parse("prepare foo (boolean) as select * from bar where active=$1");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL}, statement.dataTypes);
    assertEquals("select * from bar where active=$1", statement.originalPreparedStatement.getSql());

    statement =
        parse("prepare foo (boolean, bigint) as select * from bar where active=$1 and age=$2");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL, Oid.INT8}, statement.dataTypes);
    assertEquals(
        "select * from bar where active=$1 and age=$2",
        statement.originalPreparedStatement.getSql());

    statement =
        parse(
            "prepare foo ("
                + "\tboolean,\n"
                + "\tbigint,\n"
                + "\tcharacter varying(100)\n"
                + ") as select * from bar where active=$1 and age=$2");
    assertEquals("foo", statement.name);
    assertArrayEquals(new int[] {Oid.BOOL, Oid.INT8, Oid.VARCHAR}, statement.dataTypes);
    assertEquals(
        "select * from bar where active=$1 and age=$2",
        statement.originalPreparedStatement.getSql());

    assertThrows(PGException.class, () -> parse("prepare foo ( as select 1"));
    assertThrows(PGException.class, () -> parse("prepare foo () as select 1"));
    assertThrows(PGException.class, () -> parse("prepare (bigint) as select 1"));
    assertThrows(PGException.class, () -> parse("prepare as select 1"));
  }
}
