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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TableParserTest {

  @Test
  public void testReplaceTables() {
    Statement original = Statement.of("select * from foo");
    assertSame(original, new TableParser(original).detectAndReplaceTables(ImmutableMap.of()).y());
    assertSame(
        original,
        new TableParser(original)
            .detectAndReplaceTables(
                ImmutableMap.of(
                    new TableOrIndexName(null, "bar"), new TableOrIndexName(null, "foo")))
            .y());

    assertEquals(Statement.of("select * from bar"), replace(original, "foo", "bar"));
    assertEquals(Statement.of("select * from bar"), replace(original, "FOO", "bar"));
    assertEquals(
        Statement.of("select * from other_table"), replace(original, "Foo", "other_table"));
    assertEquals(
        Statement.of("select * from my_schema.my_table"),
        replace(original, null, "foo", "my_schema", "my_table"));
    assertEquals(
        Statement.of("select * from pg_type"),
        replace(
            Statement.of("select * from pg_catalog.pg_type"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("SELECT * FROM pg_type"),
        replace(
            Statement.of("SELECT * FROM PG_CATALOG.PG_TYPE"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("SELECT * FROM pg_type"),
        replace(
            Statement.of("SELECT * FROM \"pg_catalog\".\"pg_type\""),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("SELECT * FROM pg_catalog.\"Pg_Type\""),
        replace(
            Statement.of("SELECT * FROM pg_catalog.\"Pg_Type\""),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));

    assertEquals(
        Statement.of("select namespace, name, some_function(name) as foo from pg_type"),
        replace(
            Statement.of(
                "select namespace, name, some_function(name) as foo from pg_catalog.pg_type"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of(
            "select namespace, name, '(select max(oid) from pg_catalog.pg_type)' as foo from pg_type"),
        replace(
            Statement.of(
                "select namespace, name, '(select max(oid) from pg_catalog.pg_type)' as foo from pg_catalog.pg_type"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("select namespace, name, \"from pg_catalog.pg_type\" as foo from pg_type"),
        replace(
            Statement.of(
                "select namespace, name, \"from pg_catalog.pg_type\" as foo from pg_catalog.pg_type"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));

    assertEquals(
        Statement.of("insert into pg_type (name) values ('foo')"),
        replace(
            Statement.of("insert into pg_catalog.pg_type (name) values ('foo')"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("insert into pg_type(name)values('foo')"),
        replace(
            Statement.of("insert into pg_catalog.pg_type(name)values('foo')"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("insert   pg_type (name) values ('foo')"),
        replace(
            Statement.of("insert   pg_catalog.pg_type (name) values ('foo')"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));

    assertEquals(
        Statement.of("update pg_type set name='foo' where value='pg_type'"),
        replace(
            Statement.of("update pg_catalog.pg_type set name='foo' where value='pg_type'"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("update pg_type set name='foo' where value=(select max(value) from pg_type)"),
        replace(
            Statement.of(
                "update pg_catalog.pg_type set name='foo' where value=(select max(value) from pg_catalog.pg_type)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("update pg_type set name=(select max(name) from pg_type)"),
        replace(
            Statement.of(
                "update pg_catalog.pg_type set name=(select max(name) from \"pg_catalog\".\"pg_type\")"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));

    assertEquals(
        Statement.of("delete from pg_type"),
        replace(
            Statement.of("delete from pg_catalog.pg_type"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of("delete pg_type"),
        replace(
            Statement.of("delete pg_catalog.pg_type"), "pg_catalog", "pg_type", null, "pg_type"));

    assertEquals(
        Statement.of("select * from pg_type t1 left outer join pg_type t2 using (name)"),
        replace(
            Statement.of(
                "select * from pg_catalog.pg_type t1 left outer join pg_catalog.pg_type t2 using (name)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of(
            "select * from pg_type t1 left outer join (select name from foo) t2 using (name)"),
        replace(
            Statement.of(
                "select * from pg_catalog.pg_type t1 left outer join (select name from foo) t2 using (name)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of(
            "select * from pg_type t1 left outer join (select min(name) from pg_type) t2 using (name)"),
        replace(
            Statement.of(
                "select * from pg_catalog.pg_type t1 left outer join (select min(name) from pg_catalog.pg_type) t2 using (name)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of(
            "select * from pg_type t1 left outer join (select min(name) from foo as \"pg_catalog.pg_type\") t2 using (name)"),
        replace(
            Statement.of(
                "select * from pg_catalog.pg_type t1 left outer join (select min(name) from foo as \"pg_catalog.pg_type\") t2 using (name)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
    assertEquals(
        Statement.of(
            "select * from pg_type t1 left outer join (select min(name) from foo as \"pg_catalog.pg_type\") t2 using (name) where t1.name in (select name from pg_type)"),
        replace(
            Statement.of(
                "select * from pg_catalog.pg_type t1 left outer join (select min(name) from foo as \"pg_catalog.pg_type\") t2 using (name) where t1.name in (select name from pg_catalog.pg_type)"),
            "pg_catalog",
            "pg_type",
            null,
            "pg_type"));
  }

  @Test
  public void testReplaceMultipleTables() {
    assertEquals(
        Statement.of("select * from foo"),
        new TableParser(Statement.of("select * from foo"))
            .detectAndReplaceTables(
                ImmutableMap.of(
                    new TableOrIndexName("pg_catalog", "pg_type"),
                        new TableOrIndexName(null, "pg_type"),
                    new TableOrIndexName("pg_catalog", "pg_settings"),
                        new TableOrIndexName(null, "pg_settings"),
                    new TableOrIndexName("pg_catalog", "pg_class"),
                        new TableOrIndexName(null, "pg_class")))
            .y());
    assertEquals(
        Statement.of("select * from pg_settings"),
        new TableParser(Statement.of("select * from pg_catalog.pg_settings"))
            .detectAndReplaceTables(
                ImmutableMap.of(
                    new TableOrIndexName("pg_catalog", "pg_type"),
                        new TableOrIndexName(null, "pg_type"),
                    new TableOrIndexName("pg_catalog", "pg_settings"),
                        new TableOrIndexName(null, "pg_settings"),
                    new TableOrIndexName("pg_catalog", "pg_class"),
                        new TableOrIndexName(null, "pg_class")))
            .y());
    assertEquals(
        Statement.of("select s.name, (select min(name) from pg_type) from pg_settings s"),
        new TableParser(
                Statement.of(
                    "select s.name, (select min(name) from pg_catalog.pg_type) from pg_catalog.pg_settings s"))
            .detectAndReplaceTables(
                ImmutableMap.of(
                    new TableOrIndexName("pg_catalog", "pg_type"),
                        new TableOrIndexName(null, "pg_type"),
                    new TableOrIndexName("pg_catalog", "pg_settings"),
                        new TableOrIndexName(null, "pg_settings"),
                    new TableOrIndexName("pg_catalog", "pg_class"),
                        new TableOrIndexName(null, "pg_class")))
            .y());
    assertEquals(
        Statement.of(
            "select s.name, (select min(name) from pg_type) from pg_settings s inner join pg_class c on s.name=c.name where s.setting in (select name from pg_class)"),
        new TableParser(
                Statement.of(
                    "select s.name, (select min(name) from pg_catalog.pg_type) from pg_catalog.pg_settings s inner join pg_catalog.pg_class c on s.name=c.name where s.setting in (select name from pg_catalog.pg_class)"))
            .detectAndReplaceTables(
                ImmutableMap.of(
                    new TableOrIndexName("pg_catalog", "pg_type"),
                        new TableOrIndexName(null, "pg_type"),
                    new TableOrIndexName("pg_catalog", "pg_settings"),
                        new TableOrIndexName(null, "pg_settings"),
                    new TableOrIndexName("pg_catalog", "pg_class"),
                        new TableOrIndexName(null, "pg_class")))
            .y());
  }

  static Statement replace(Statement original, String table, String withTable) {
    return replace(original, null, table, null, withTable);
  }

  static Statement replace(
      Statement original, String schema, String table, String withSchema, String withTable) {
    return new TableParser(original)
        .detectAndReplaceTables(
            ImmutableMap.of(
                new TableOrIndexName(schema, table), new TableOrIndexName(withSchema, withTable)))
        .y();
  }
}
