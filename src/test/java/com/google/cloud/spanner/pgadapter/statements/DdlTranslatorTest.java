// Copyright 2023 Google LLC
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

import static com.google.cloud.spanner.pgadapter.statements.DdlTranslator.translate;
import static com.google.cloud.spanner.pgadapter.statements.DdlTranslator.translateCreateView;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DdlTranslatorTest {

  @Test
  public void testDropTable() {
    assertEquals("DROP TABLE my_table", translate("DROP TABLE my_table"));
    // This is an invalid statement, but the point is that the translate function should just be
    // a passthrough in that case.
    assertEquals(
        "DROP TABLE my_table INTERLEAVE IN PARENT foo",
        translate("DROP TABLE my_table INTERLEAVE IN PARENT foo"));
  }

  @Test
  public void testTranslateCreateTable() {
    assertEquals(
        "CREATE TABLE my_table (id bigint primary key, value varchar)",
        translate("CREATE TABLE my_table (id bigint primary key, value varchar)"));
    assertEquals(
        "CREATE TABLE my_table (id bigint primary key, value varchar) /* INTERLEAVE IN PARENT foo */",
        translate(
            "CREATE TABLE my_table (id bigint primary key, value varchar) INTERLEAVE IN PARENT foo"));
    assertEquals(
        "CREATE TABLE my_table (id bigint primary key, value varchar) /* TTL INTERVAL '3 days' ON last_updated_at */",
        translate(
            "CREATE TABLE my_table (id bigint primary key, value varchar) TTL INTERVAL '3 days' ON last_updated_at"));
    assertEquals(
        "CREATE TABLE my_table (id bigint primary key, value varchar) /* INTERLEAVE IN PARENT foo TTL INTERVAL '3 days' ON last_updated_at */",
        translate(
            "CREATE TABLE my_table (id bigint primary key, value varchar) INTERLEAVE IN PARENT foo TTL INTERVAL '3 days' ON last_updated_at"));
    assertEquals(
        "CREATE TABLE my_table (id bigint primary key, value varchar) /* TTL INTERVAL '3 days' ON last_updated_at INTERLEAVE IN PARENT foo */",
        translate(
            "CREATE TABLE my_table (id bigint primary key, value varchar) TTL INTERVAL '3 days' ON last_updated_at INTERLEAVE IN PARENT foo"));
    assertEquals(
        "CREATE TABLE if not exists my_table (id bigint primary key, value varchar) /* TTL INTERVAL '3 days' ON last_updated_at INTERLEAVE IN PARENT foo */",
        translate(
            "CREATE TABLE if not exists my_table (id bigint primary key, value varchar) TTL INTERVAL '3 days' ON last_updated_at INTERLEAVE IN PARENT foo"));
  }

  @Test
  public void testTranslateCreateView() {
    assertEquals(
        "CREATE OR REPLACE VIEW my_view /* sql security invoker */ as select id, value from my_table",
        translate(
            "CREATE OR REPLACE VIEW my_view sql security invoker as select id, value from my_table"));
    assertEquals(
        "CREATE VIEW my_view /* sql security invoker */ as select id, value from my_table",
        translate("CREATE VIEW my_view sql security invoker as select id, value from my_table"));
    assertEquals(
        "CREATE VIEW my_view /* /* sql security invoker */ sql security invoker */ as select id, value from my_table",
        translate(
            "CREATE VIEW my_view /* sql security invoker */ sql security invoker as select id, value from my_table"));
    assertEquals(
        "CREATE VIEW my_view as select id, value from my_table",
        translate("CREATE VIEW my_view as select id, value from my_table"));
    assertEquals(
        "CREATE VIEW my_view sql security definer as select id, value from my_table",
        translate("CREATE VIEW my_view sql security definer as select id, value from my_table"));

    assertEquals("foo", translateCreateView("foo"));
    assertEquals("create view foo", translateCreateView("create view foo"));
    assertEquals("create view foo as select 1", translateCreateView("create view foo as select 1"));
    assertEquals(
        "create view foo /* sql security invoker */ as select 1",
        translateCreateView("create view foo sql security invoker as select 1"));
    assertEquals("create or replace view foo", translateCreateView("create or replace view foo"));
    assertEquals("create replace view foo", translateCreateView("create replace view foo"));
    assertEquals("create or view foo", translateCreateView("create or view foo"));
    assertEquals(
        "create or replace view sql security invoker as select 1",
        translateCreateView("create or replace view sql security invoker as select 1"));
    assertEquals("create view", translateCreateView("create view"));
  }

  @Test
  public void testTranslateCreateIndex() {
    assertEquals(
        "CREATE INDEX my_index ON my_table (value)",
        translate("CREATE INDEX my_index ON my_table (value)"));
    assertEquals(
        "CREATE UNIQUE INDEX my_index ON my_table (value)",
        translate("CREATE UNIQUE INDEX my_index ON my_table (value)"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) WHERE foo=1",
        translate("CREATE INDEX my_index ON my_table (value) WHERE foo=1"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) INCLUDE (foo)",
        translate("CREATE INDEX my_index ON my_table (value) INCLUDE (foo)"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) /* INTERLEAVE IN my_other_table */",
        translate("CREATE INDEX my_index ON my_table (value) INTERLEAVE IN my_other_table"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) INCLUDE (foo) /* INTERLEAVE IN my_other_table */",
        translate(
            "CREATE INDEX my_index ON my_table (value) INCLUDE (foo) INTERLEAVE IN my_other_table"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) /* INTERLEAVE IN my_other_table */ WHERE foo=1",
        translate(
            "CREATE INDEX my_index ON my_table (value) INTERLEAVE IN my_other_table WHERE foo=1"));
    assertEquals(
        "CREATE INDEX my_index ON my_table (value) include (foo) /* INTERLEAVE IN my_other_table */ WHERE foo=1",
        translate(
            "CREATE INDEX my_index ON my_table (value) include (foo) INTERLEAVE IN my_other_table WHERE foo=1"));
    assertEquals(
        "CREATE INDEX IF NOT EXISTS my_index ON my_table (value) include (foo) /* INTERLEAVE IN my_other_table */ WHERE foo=1",
        translate(
            "CREATE INDEX IF NOT EXISTS my_index ON my_table (value) include (foo) INTERLEAVE IN my_other_table WHERE foo=1"));
  }

  @Test
  public void testTranslateCreateChangeStream() {
    assertEquals(
        "/* CREATE CHANGE STREAM my_stream for col1, col2 */",
        translate("CREATE CHANGE STREAM my_stream for col1, col2"));
    assertEquals(
        "/* CREATE CHANGE STREAM my_stream for col1, col2 WITH (retention_period = 'duration') */",
        translate(
            "CREATE CHANGE STREAM my_stream for col1, col2 WITH (retention_period = 'duration')"));
  }
}
