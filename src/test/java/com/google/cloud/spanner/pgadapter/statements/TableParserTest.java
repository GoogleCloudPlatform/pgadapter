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
import com.google.common.collect.ImmutableMap.Builder;
import org.junit.Ignore;
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
        Statement.of("select * from bar"),
        replace(Statement.of("select * from \"foo\""), "foo", "bar"));
    assertEquals(
        Statement.of("select * from \"Foo\""),
        replace(Statement.of("select * from \"Foo\""), "foo", "bar"));
    assertEquals(
        Statement.of("select * from my_schema.bar"),
        replace(Statement.of("select * from public.foo"), "public", "foo", "my_schema", "bar"));
    assertEquals(
        Statement.of("select * from my_schema.bar"),
        replace(Statement.of("select * from public.\"foo\""), "public", "foo", "my_schema", "bar"));
    assertEquals(
        Statement.of("select * from my_schema.bar"),
        replace(Statement.of("select * from \"public\".foo"), "public", "foo", "my_schema", "bar"));
    assertEquals(
        Statement.of("select * from my_schema.bar"),
        replace(
            Statement.of("select * from \"public\".\"foo\""), "public", "foo", "my_schema", "bar"));
    assertEquals(
        Statement.of("select * from \"Public\".\"foo\""),
        replace(
            Statement.of("select * from \"Public\".\"foo\""), "public", "foo", "my_schema", "bar"));
    assertEquals(
        Statement.of("select * from \"public\".\"Foo\""),
        replace(
            Statement.of("select * from \"public\".\"Foo\""), "public", "foo", "my_schema", "bar"));

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
    assertEquals(
        Statement.of(
            "SELECT is(\n"
                + "    ARRAY(\n"
                + "        SELECT stuff_id\n"
                + "          FROM replaced_domain_stuff\n"
                + "         WHERE domain_id = $1\n"
                + "           AND src_id = $2\n"
                + "         ORDER BY stuff_id\n"
                + "    ),\n"
                + "    ARRAY[ 1, 2, 3 ],\n"
                + "    'some-string-literal'\n"
                + ");"),
        replace(
            Statement.of(
                "SELECT is(\n"
                    + "    ARRAY(\n"
                    + "        SELECT stuff_id\n"
                    + "          FROM replaced_domain_stuff\n"
                    + "         WHERE domain_id = $1\n"
                    + "           AND src_id = $2\n"
                    + "         ORDER BY stuff_id\n"
                    + "    ),\n"
                    + "    ARRAY[ 1, 2, 3 ],\n"
                    + "    'some-string-literal'\n"
                    + ");"),
            "domain_stuff",
            "replaced_domain_stuff"));
    assertEquals(
        Statement.of(
            "select abc, sub.key from "
                + "(select key, 'abc' as abc from replaced) sub "
                + "order by 1"),
        replace(
            Statement.of(
                "select abc, sub.key from "
                    + "(select key, 'abc' as abc from keyvalue) sub "
                    + "order by 1"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "insert into \"AllSpangresTypes\"(string_value, bool_value, "
                + "int64_value) "
                + "values ('hello world', default, 1234567890123), (default, true, "
                + "9876543210987)"),
        replace(
            Statement.of(
                "insert into keyvalue(string_value, bool_value, "
                    + "int64_value) "
                    + "values ('hello world', default, 1234567890123), (default, true, "
                    + "9876543210987)"),
            "keyvalue",
            "\"AllSpangresTypes\""));
    assertEquals(
        Statement.of(
            "update replaced set value='v'where key=12345678912 "
                + "returning key + 1 as newkey, value as newvalue"),
        replace(
            Statement.of(
                "update keyvalue set value='v'where key=12345678912 "
                    + "returning key + 1 as newkey, value as newvalue"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "select\n"
                + "            subquery.value, subquery.key, subquery.f1\n"
                + "          from (\n"
                + "            select\n"
                + "              key, value, 123456789012 as f1\n"
                + "            from\n"
                + "              replaced\n"
                + "          ) subquery"),
        replace(
            Statement.of(
                "select\n"
                    + "            subquery.value, subquery.key, subquery.f1\n"
                    + "          from (\n"
                    + "            select\n"
                    + "              key, value, 123456789012 as f1\n"
                    + "            from\n"
                    + "              keyvalue\n"
                    + "          ) subquery"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of("select exists(select 1000000000000 as `?column?`) " + "as exists_col"),
        replace(
            Statement.of("select exists(select 1000000000000 as `?column?`) " + "as exists_col"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "select * from replaced where key > 5 and not(key > 10) and "
                + "value='hello' and (key = 6 or value='world')"),
        replace(
            Statement.of(
                "select * from keyvalue where key > 5 and not(key > 10) and "
                    + "value='hello' and (key = 6 or value='world')"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "select CASE pg.map_double_to_int(double_value) WHEN "
                + "pg.map_double_to_int(double_value) THEN "
                + "double_value END as result from replaced"),
        replace(
            Statement.of(
                "select CASE pg.map_double_to_int(double_value) WHEN "
                    + "pg.map_double_to_int(double_value) THEN "
                    + "double_value END as result from AllSpangresTypes"),
            "allspangrestypes",
            "replaced"));
    assertEquals(
        Statement.of(
            "select 1 as one from replaced join /*@ join_hint = 1 */ replaced kv2 on true"),
        replace(
            Statement.of(
                "select 1 as one from keyvalue join /*@ join_hint = 1 "
                    + "*/ keyvalue kv2 on true"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "with t1 as (select key kk, value vv from replaced), "
                + "t2 as (select kk xx, vv from t1) "
                + "select xx, vv, '--' as col, * from t2"),
        replace(
            Statement.of(
                "with t1 as (select key kk, value vv from keyvalue), "
                    + "t2 as (select kk xx, vv from t1) "
                    + "select xx, vv, '--' as col, * from t2"),
            "keyvalue",
            "replaced"));
    assertEquals(
        Statement.of(
            "with t as (select key from replaced), "
                + "t2 as (select 123 as col from replaced) "
                + "select value from replaced"),
        replace(
            Statement.of(
                "with t as (select key from keyvalue), "
                    + "t2 as (select 123 as col from keyvalue) "
                    + "select value from keyvalue"),
            "keyvalue",
            "replaced"));
  }

  @Test
  public void testReplacePgCatalogTables() {
    assertEquals(
        Statement.of(
            "SELECT n.nspname as \"Schema\",\n"
                + "          c.relname as \"Name\",\n"
                + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
                + "        FROM pg_class c\n"
                + "             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                + "        WHERE c.relkind IN ('r','p','v','m','S','f','')\n"
                + "              AND n.nspname <> 'pg_catalog'\n"
                + "              AND n.nspname <> 'information_schema'\n"
                + "              AND n.nspname !~ '^pg_toast'\n"
                + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                + "        ORDER BY 1,2;"),
        removePgCatalog(
            Statement.of(
                "SELECT n.nspname as \"Schema\",\n"
                    + "          c.relname as \"Name\",\n"
                    + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                    + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
                    + "        FROM pg_catalog.pg_class c\n"
                    + "             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
                    + "        WHERE c.relkind IN ('r','p','v','m','S','f','')\n"
                    + "              AND n.nspname <> 'pg_catalog'\n"
                    + "              AND n.nspname <> 'information_schema'\n"
                    + "              AND n.nspname !~ '^pg_toast'\n"
                    + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                    + "        ORDER BY 1,2;"),
            "pg_class",
            "pg_namespace"));
    assertEquals(
        Statement.of(
            "SELECT c.oid,\n"
                + "          n.nspname,\n"
                + "          c.relname\n"
                + "        FROM pg_class c\n"
                + "             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                + "        WHERE c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                + "        ORDER BY 2, 3;\n"),
        removePgCatalog(
            Statement.of(
                "SELECT c.oid,\n"
                    + "          n.nspname,\n"
                    + "          c.relname\n"
                    + "        FROM pg_catalog.pg_class c\n"
                    + "             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
                    + "        WHERE c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                    + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                    + "        ORDER BY 2, 3;\n"),
            "pg_class",
            "pg_namespace"));
    assertEquals(
        Statement.of(
            "SELECT n.nspname as \"Schema\",\n"
                + "          c.relname as \"Name\",\n"
                + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
                + "        FROM pg_class c\n"
                + "             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                + "        WHERE c.relkind IN ('r','p','s','')\n"
                + "              AND n.nspname !~ '^pg_toast'\n"
                + "          AND c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                + "        ORDER BY 1,2;"),
        removePgCatalog(
            Statement.of(
                "SELECT n.nspname as \"Schema\",\n"
                    + "          c.relname as \"Name\",\n"
                    + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                    + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
                    + "        FROM pg_catalog.pg_class c\n"
                    + "             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
                    + "        WHERE c.relkind IN ('r','p','s','')\n"
                    + "              AND n.nspname !~ '^pg_toast'\n"
                    + "          AND c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                    + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                    + "        ORDER BY 1,2;"),
            "pg_class",
            "pg_namespace"));
    assertEquals(
        Statement.of(
            "SELECT n.nspname as \"Schema\",\n"
                + "          c.relname as \"Name\",\n"
                + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",\n"
                + "          c2.relname as \"Table\"\n"
                + "        FROM pg_class c\n"
                + "             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                + "             LEFT JOIN pg_index i ON i.indexrelid = c.oid\n"
                + "             LEFT JOIN pg_class c2 ON i.indrelid = c2.oid\n"
                + "        WHERE c.relkind IN ('i','I','s','')\n"
                + "              AND n.nspname !~ '^pg_toast'\n"
                + "          AND c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                + "        ORDER BY 1,2;"),
        removePgCatalog(
            Statement.of(
                "SELECT n.nspname as \"Schema\",\n"
                    + "          c.relname as \"Name\",\n"
                    + "          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
                    + "          pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",\n"
                    + "          c2.relname as \"Table\"\n"
                    + "        FROM pg_catalog.pg_class c\n"
                    + "             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
                    + "             LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n"
                    + "             LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid\n"
                    + "        WHERE c.relkind IN ('i','I','s','')\n"
                    + "              AND n.nspname !~ '^pg_toast'\n"
                    + "          AND c.relname OPERATOR(pg_catalog.~) '^(test)$' COLLATE pg_catalog.default\n"
                    + "          AND pg_catalog.pg_table_is_visible(c.oid)\n"
                    + "        ORDER BY 1,2;"),
            "pg_class",
            "pg_namespace",
            "pg_index"));
    assertEquals(
        Statement.of(
            "SELECT d.datname as \"Name\",\n"
                + "               pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
                + "               pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
                + "               d.datcollate as \"Collate\",\n"
                + "               d.datctype as \"Ctype\",\n"
                + "               pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\"\n"
                + "        FROM pg_database d\n"
                + "        ORDER BY 1;"),
        removePgCatalog(
            Statement.of(
                "SELECT d.datname as \"Name\",\n"
                    + "               pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
                    + "               pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
                    + "               d.datcollate as \"Collate\",\n"
                    + "               d.datctype as \"Ctype\",\n"
                    + "               pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\"\n"
                    + "        FROM pg_catalog.pg_database d\n"
                    + "        ORDER BY 1;"),
            "pg_database"));
    assertEquals(
        Statement.of(
            "SELECT pg_catalog.quote_ident(c.relname) FROM pg_class c WHERE c.relkind IN ('r', 'S', 'v', 'm', 'f', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,0)='' AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')\n"
                + "        UNION\n"
                + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_namespace n WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,0)='' AND (SELECT pg_catalog.count(*) FROM pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
                + "        UNION\n"
                + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'S', 'v', 'm', 'f', 'p') AND substring(pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname),1,0)='' AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND (SELECT pg_catalog.count(*) FROM pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
                + "        LIMIT 1000\n"),
        removePgCatalog(
            Statement.of(
                "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN ('r', 'S', 'v', 'm', 'f', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,0)='' AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
                    + "        UNION\n"
                    + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,0)='' AND (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
                    + "        UNION\n"
                    + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'S', 'v', 'm', 'f', 'p') AND substring(pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname),1,0)='' AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
                    + "        LIMIT 1000\n"),
            "pg_class",
            "pg_namespace"));
    assertEquals(
        Statement.of(
            "SELECT a.attname,\n"
                + "          pg_catalog.format_type(a.atttypid, a.atttypmod),\n"
                + "          (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)\n"
                + "           FROM pg_attrdef d\n"
                + "           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n"
                + "          a.attnotnull,\n"
                + "          (SELECT c.collname FROM pg_collation c, pg_type t\n"
                + "           WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,\n"
                + "          a.attidentity,\n"
                + "          a.attgenerated\n"
                + "        FROM pg_attribute a\n"
                + "        WHERE a.attrelid = '16404' AND a.attnum > 0 AND NOT a.attisdropped\n"
                + "        ORDER BY a.attnum;"),
        removePgCatalog(
            Statement.of(
                "SELECT a.attname,\n"
                    + "          pg_catalog.format_type(a.atttypid, a.atttypmod),\n"
                    + "          (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)\n"
                    + "           FROM pg_catalog.pg_attrdef d\n"
                    + "           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n"
                    + "          a.attnotnull,\n"
                    + "          (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t\n"
                    + "           WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,\n"
                    + "          a.attidentity,\n"
                    + "          a.attgenerated\n"
                    + "        FROM pg_catalog.pg_attribute a\n"
                    + "        WHERE a.attrelid = '16404' AND a.attnum > 0 AND NOT a.attisdropped\n"
                    + "        ORDER BY a.attnum;"),
            "pg_attribute",
            "pg_collation",
            "pg_type",
            "pg_attrdef"));
    assertEquals(
        Statement.of(
            "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n"
                + "          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace\n"
                + "        FROM pg_class c, pg_class c2, pg_index i\n"
                + "          LEFT JOIN pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))\n"
                + "        WHERE c.oid = '16404' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\n"
                + "        ORDER BY i.indisprimary DESC, c2.relname;"),
        removePgCatalog(
            Statement.of(
                "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n"
                    + "          pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace\n"
                    + "        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i\n"
                    + "          LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))\n"
                    + "        WHERE c.oid = '16404' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\n"
                    + "        ORDER BY i.indisprimary DESC, c2.relname;"),
            "pg_class",
            "pg_index",
            "pg_constraint"));
    assertEquals(
        Statement.of(
            "SELECT pol.polname, pol.polpermissive,\n"
                + "          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_roles where oid = any (pol.polroles) order by 1),',') END,\n"
                + "          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),\n"
                + "          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),\n"
                + "          CASE pol.polcmd\n"
                + "            WHEN 'r' THEN 'SELECT'\n"
                + "            WHEN 'a' THEN 'INSERT'\n"
                + "            WHEN 'w' THEN 'UPDATE'\n"
                + "            WHEN 'd' THEN 'DELETE'\n"
                + "            END AS cmd\n"
                + "        FROM pg_policy pol\n"
                + "        WHERE pol.polrelid = '16404' ORDER BY 1;"),
        removePgCatalog(
            Statement.of(
                "SELECT pol.polname, pol.polpermissive,\n"
                    + "          CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END,\n"
                    + "          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid),\n"
                    + "          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid),\n"
                    + "          CASE pol.polcmd\n"
                    + "            WHEN 'r' THEN 'SELECT'\n"
                    + "            WHEN 'a' THEN 'INSERT'\n"
                    + "            WHEN 'w' THEN 'UPDATE'\n"
                    + "            WHEN 'd' THEN 'DELETE'\n"
                    + "            END AS cmd\n"
                    + "        FROM pg_catalog.pg_policy pol\n"
                    + "        WHERE pol.polrelid = '16404' ORDER BY 1;"),
            "pg_class",
            "pg_index",
            "pg_constraint",
            "pg_policy",
            "pg_roles"));
    assertEquals(
        Statement.of(
            "SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,\n"
                + "          (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')\n"
                + "           FROM pg_catalog.unnest(stxkeys) s(attnum)\n"
                + "           JOIN pg_attribute a ON (stxrelid = a.attrelid AND\n"
                + "                a.attnum = s.attnum AND NOT attisdropped)) AS columns,\n"
                + "          'd' = any(stxkind) AS ndist_enabled,\n"
                + "          'f' = any(stxkind) AS deps_enabled,\n"
                + "          'm' = any(stxkind) AS mcv_enabled,\n"
                + "          stxstattarget\n"
                + "        FROM pg_statistic_ext stat\n"
                + "        WHERE stxrelid = '16404'\n"
                + "        ORDER BY 1"),
        removePgCatalog(
            Statement.of(
                "SELECT oid, stxrelid::pg_catalog.regclass, stxnamespace::pg_catalog.regnamespace AS nsp, stxname,\n"
                    + "          (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(attname),', ')\n"
                    + "           FROM pg_catalog.unnest(stxkeys) s(attnum)\n"
                    + "           JOIN pg_catalog.pg_attribute a ON (stxrelid = a.attrelid AND\n"
                    + "                a.attnum = s.attnum AND NOT attisdropped)) AS columns,\n"
                    + "          'd' = any(stxkind) AS ndist_enabled,\n"
                    + "          'f' = any(stxkind) AS deps_enabled,\n"
                    + "          'm' = any(stxkind) AS mcv_enabled,\n"
                    + "          stxstattarget\n"
                    + "        FROM pg_catalog.pg_statistic_ext stat\n"
                    + "        WHERE stxrelid = '16404'\n"
                    + "        ORDER BY 1"),
            "pg_class",
            "pg_index",
            "pg_constraint",
            "pg_policy",
            "pg_roles",
            "pg_statistic_ext",
            "pg_attribute"));
    assertEquals(
        Statement.of(
            "SELECT pg_catalog.quote_ident(c.relname) FROM pg_class c WHERE c.relkind IN ('r', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,0)='' AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')\n"
                + "        UNION\n"
                + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_namespace n WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,0)='' AND (SELECT pg_catalog.count(*) FROM pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
                + "        UNION\n"
                + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) FROM pg_class c, pg_namespace n WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'p') AND substring(pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname),1,0)='' AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND (SELECT pg_catalog.count(*) FROM pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
                + "        LIMIT 1000"),
        removePgCatalog(
            Statement.of(
                "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN ('r', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,0)='' AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
                    + "        UNION\n"
                    + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE substring(pg_catalog.quote_ident(n.nspname) || '.',1,0)='' AND (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
                    + "        UNION\n"
                    + "        SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'p') AND substring(pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname),1,0)='' AND substring(pg_catalog.quote_ident(n.nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE substring(pg_catalog.quote_ident(nspname) || '.',1,0) = substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
                    + "        LIMIT 1000"),
            "pg_class",
            "pg_index",
            "pg_constraint",
            "pg_policy",
            "pg_roles",
            "pg_statistic_ext",
            "pg_attribute",
            "pg_namespace"));
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

  @Ignore("These constructs are not yet supported")
  @Test
  public void testNestedTableExpressions() {
    assertEquals(
        Statement.of(
            "select kv.key, kv2.value2, kv3.value, kv4.key from\n"
                + "  replaced kv left join \"KeyValue2\" kv2 on kv.value = kv2.value2,\n"
                + "  (replaced kv3 join replaced kv4 on kv3.key = kv4.key\n"
                + "    join replaced kv5 on kv4.value = kv5.value);\n"
                + "select kv.key, kv2.value2, kv3.value, kv4.key from\n"
                + "  replaced kv left join KeyValue2 kv2 on kv.value = kv2.value2,\n"
                + "  (replaced kv3 join replaced kv4 on kv3.key = kv4.key\n"
                + "    join replaced kv5 on kv4.value = kv5.value)"),
        replace(
            Statement.of(
                "select kv.key, kv2.value2, kv3.value, kv4.key from\n"
                    + "  keyvalue kv left join \"KeyValue2\" kv2 on kv.value = kv2.value2,\n"
                    + "  (keyvalue kv3 join keyvalue kv4 on kv3.key = kv4.key\n"
                    + "    join keyvalue kv5 on kv4.value = kv5.value);\n"
                    + "select kv.key, kv2.value2, kv3.value, kv4.key from\n"
                    + "  keyvalue kv left join KeyValue2 kv2 on kv.value = kv2.value2,\n"
                    + "  (keyvalue kv3 join keyvalue kv4 on kv3.key = kv4.key\n"
                    + "    join keyvalue kv5 on kv4.value = kv5.value)"),
            "keyvalue",
            "replaced"));
  }

  static Statement removePgCatalog(Statement original, String... tables) {
    Builder<TableOrIndexName, TableOrIndexName> builder = ImmutableMap.builder();
    for (String table : tables) {
      builder.put(new TableOrIndexName("pg_catalog", table), new TableOrIndexName(null, table));
    }
    return new TableParser(original).detectAndReplaceTables(builder.build()).y();
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
