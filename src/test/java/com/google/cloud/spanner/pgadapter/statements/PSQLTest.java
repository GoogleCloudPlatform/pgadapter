// Copyright 2020 Google LLC
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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.CommandMetadataParser;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class PSQLTest {
  private static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);

  private static ParsedStatement parse(String sql) {
    return PARSER.parse(Statement.of(sql));
  }

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private ConnectionHandler connectionHandler;

  @Mock private Connection connection;

  @Mock private ProxyServer server;

  @Mock private OptionsMetadata options;

  @Before
  public void setup() throws Exception {
    final JSONObject defaultCommands = new CommandMetadataParser().defaultCommands();
    Mockito.when(connectionHandler.getSpannerConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.getCommandMetadataJSON()).thenReturn(defaultCommands);
  }

  private String translate(String sql) {
    return SimpleQueryStatement.translatePotentialMetadataCommand(parse(sql), connectionHandler)
        .getSqlWithoutComments();
  }

  @Test
  public void testDescribeTranslates() {
    // PSQL equivalent: \d
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','v','m','S','f','')\n"
            + "      AND n.nspname <> 'pg_catalog'\n"
            + "      AND n.nspname <> 'information_schema'\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT"
            + " t.table_schema as \"Schema\","
            + " t.table_name as \"Name\","
            + " 'table' as \"Type\","
            + " 'me' as \"Owner\" "
            + "FROM"
            + " information_schema.tables AS t "
            + "WHERE"
            + " t.table_schema = 'public'";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableMatchTranslates() {
    // PSQL equivalent: \d <table> (1)
    String sql =
        "SELECT c.oid,\n"
            + "  n.nspname,\n"
            + "  c.relname\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 2, 3;";
    String expected =
        "SELECT"
            + " t.table_name as oid,"
            + " 'public' as nspname,"
            + " t.table_name as relname"
            + " FROM"
            + " information_schema.tables AS t"
            + " WHERE"
            + " t.table_schema='public'"
            + " AND"
            + " LOWER(t.table_name) = LOWER('users')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableMatchHandlesBobbyTables() {
    // PSQL equivalent: \d <table> (1)
    String sql =
        "SELECT c.oid,\n"
            + "  n.nspname,\n"
            + "  c.relname\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 2, 3;";
    String expected =
        "SELECT"
            + " t.table_name as oid,"
            + " 'public' as nspname,"
            + " t.table_name as relname"
            + " FROM"
            + " information_schema.tables AS t"
            + " WHERE"
            + " t.table_schema='public'"
            + " AND"
            + " LOWER(t.table_name) = LOWER('bobby''; DROP TABLE USERS; SELECT''')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableCatalogTranslates() {
    // PSQL equivalent: \d <table> (2)
    String sql =
        "SELECT relchecks, relkind, relhasindex, relhasrules, reltriggers <> 0, false, false,"
            + " relhasoids, false as relispartition, '', ''\n"
            + "FROM pg_catalog.pg_class WHERE oid = '-2264987671676060158';";
    String expected =
        "SELECT"
            + " 0 as relcheck,"
            + " 'r' as relkind,"
            + " false as relhasindex,"
            + " false as relhasrules,"
            + " false as reltriggers,"
            + " false as bool1,"
            + " false as bool2,"
            + " false as relhasoids,"
            + " '' as str1,"
            + " '' as str2";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableMetadataTranslates() {
    // PSQL equivalent: \d <table> (3)
    String sql =
        "SELECT a.attname,\n"
            + "  pg_catalog.format_type(a.atttypid, a.atttypmod),\n"
            + "  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)\n"
            + "   FROM pg_catalog.pg_attrdef d\n"
            + "   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n"
            + "  a.attnotnull,\n"
            + "  NULL AS attcollation,\n"
            + "  ''::pg_catalog.char AS attidentity,\n"
            + "  ''::pg_catalog.char AS attgenerated\n"
            + "FROM pg_catalog.pg_attribute a\n"
            + "WHERE a.attrelid = '-1' AND a.attnum > 0 AND NOT a.attisdropped\n"
            + "ORDER BY a.attnum;";
    String expected =
        "SELECT"
            + " t.column_name as attname,"
            + " t.data_type as format_type,"
            + " '' as substring,"
            + " t.is_nullable = 'NO' as attnotnull,"
            + " null::INTEGER as attcollation,"
            + " null::INTEGER as indexdef,"
            + " null::INTEGER as attfdwoptions"
            + " FROM"
            + " information_schema.columns AS t"
            + " WHERE"
            + " t.table_schema='public'"
            + " AND t.table_name = '-1'";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableMetadataHandlesBobbyTables() {
    // PSQL equivalent: \d <table> (3)
    String sql =
        "SELECT a.attname,\n"
            + "  pg_catalog.format_type(a.atttypid, a.atttypmod),\n"
            + "  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)\n"
            + "   FROM pg_catalog.pg_attrdef d\n"
            + "   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n"
            + "  a.attnotnull,\n"
            + "  NULL AS attcollation,\n"
            + "  ''::pg_catalog.char AS attidentity,\n"
            + "  ''::pg_catalog.char AS attgenerated\n"
            + "FROM pg_catalog.pg_attribute a\n"
            + "WHERE a.attrelid = 'bobby'; DROP TABLE USERS; SELECT'' AND a.attnum > 0 AND NOT"
            + " a.attisdropped\n"
            + "ORDER BY a.attnum;";
    String expected =
        "SELECT"
            + " t.column_name as attname,"
            + " t.data_type as format_type,"
            + " '' as substring,"
            + " t.is_nullable = 'NO' as attnotnull,"
            + " null::INTEGER as attcollation,"
            + " null::INTEGER as indexdef,"
            + " null::INTEGER as attfdwoptions"
            + " FROM"
            + " information_schema.columns AS t"
            + " WHERE"
            + " t.table_schema='public'"
            + " AND t.table_name = 'bobby''; DROP TABLE USERS; SELECT'''";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableAttributesTranslates() {
    // PSQL equivalent: \d <table> (4)
    String sql =
        "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i"
            + " WHERE c.oid=i.inhparent AND i.inhrelid = '-2264987671676060158' AND c.relkind !="
            + " 'p' ORDER BY inhseqno;";
    String expected = "SELECT 1 LIMIT 0";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeMoreTableAttributesTranslates() {
    // PSQL equivalent: \d <table> (5)
    String sql =
        "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i"
            + " WHERE c.oid=i.inhrelid AND i.inhparent = '-2264987671676060158' ORDER BY"
            + " c.relname;";
    String expected = "SELECT 1 LIMIT 0";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testListCommandTranslates() {
    // PSQL equivalent: \l
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "ORDER BY 1;";
    String expected = "SELECT '' AS Name";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testListDatabaseCommandTranslates() {
    // PSQL equivalent: \l <table>
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "WHERE d.datname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "ORDER BY 1;";
    String expected = "SELECT '' AS Name";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testListDatabaseCommandFailsButPrintsUnknown() {
    // PSQL equivalent: \l <table>
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "WHERE d.datname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "ORDER BY 1;";
    String expected = "SELECT '' AS Name";

    // TODO: Add Connection#getDatabase() to Connection API and test here what happens if that
    // method throws an exception.
    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeAllTableMetadataTranslates() {
    // PSQL equivalent: \dt
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','')\n"
            + "      AND n.nspname <> 'pg_catalog'\n"
            + "      AND n.nspname <> 'information_schema'\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected = "SELECT * FROM information_schema.tables";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedTableMetadataTranslates() {
    // PSQL equivalent: \dt <table>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT * FROM information_schema.tables WHERE LOWER(table_name) = LOWER('users')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedTableMetadataHandlesBobbyTables() {
    // PSQL equivalent: \dt <table>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT * FROM information_schema.tables WHERE LOWER(table_name) ="
            + " LOWER('bobby''; DROP TABLE USERS; SELECT''')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeAllIndexMetadataTranslates() {
    // PSQL equivalent: \di
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",\n"
            + " c2.relname as \"Table\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n"
            + "     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid\n"
            + "WHERE c.relkind IN ('i','I','')\n"
            + "      AND n.nspname <> 'pg_catalog'\n"
            + "      AND n.nspname <> 'information_schema'\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT table_catalog, table_schema, table_name, index_name, index_type, parent_table_name, is_unique, is_null_filtered, index_state, spanner_is_managed FROM information_schema.indexes";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedIndexMetadataTranslates() {
    // PSQL equivalent: \di <index>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",\n"
            + " c2.relname as \"Table\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n"
            + "     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid\n"
            + "WHERE c.relkind IN ('i','I','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(index)$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT table_catalog, table_schema, table_name, index_name, index_type, parent_table_name, is_unique, is_null_filtered, index_state, spanner_is_managed FROM information_schema.indexes WHERE LOWER(index_name) ="
            + " LOWER('index')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedIndexMetadataHandlesBobbyTables() {
    // PSQL equivalent: \di <index>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN"
            + " 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN"
            + " 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I'"
            + " THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",\n"
            + " c2.relname as \"Table\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid\n"
            + "     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid\n"
            + "WHERE c.relkind IN ('i','I','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String expected =
        "SELECT table_catalog, table_schema, table_name, index_name, index_type, parent_table_name, is_unique, is_null_filtered, index_state, spanner_is_managed FROM information_schema.indexes WHERE LOWER(index_name) ="
            + " LOWER('bobby''; DROP TABLE USERS; SELECT''')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeAllSchemaMetadataTranslates() {
    // PSQL equivalent: \dn
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'\n"
            + "ORDER BY 1;";
    String expected = "SELECT * FROM information_schema.schemata";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedSchemaMetadataTranslates() {
    // PSQL equivalent: \dn <schema>
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname OPERATOR(pg_catalog.~) '^(schema)$'\n"
            + "ORDER BY 1;";
    String expected =
        "SELECT * FROM information_schema.schemata WHERE LOWER(schema_name) = LOWER('schema')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSelectedSchemaMetadataHandlesBobbyTables() {
    // PSQL equivalent: \dn <schema>
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "ORDER BY 1;";
    String expected =
        "SELECT * FROM information_schema.schemata WHERE LOWER(schema_name) = LOWER('bobby''; DROP"
            + " TABLE USERS; SELECT''')";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testTableSelectAutocomplete() {
    // PSQL equivalent: SELECT <table>
    String sql =
        "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN"
            + " ('r', 'S', 'v', 'm', 'f', 'p') AND"
            + " substring(pg_catalog.quote_ident(c.relname),1,0)='user' AND"
            + " pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM"
            + " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,0)='' AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,0) ="
            + " substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)"
            + " FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid"
            + " AND c.relkind IN ('r', 'S', 'v', 'm', 'f', 'p') AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.' ||"
            + " pg_catalog.quote_ident(c.relname),1,0)='' AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,0) ="
            + " substring('',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,0) ="
            + " substring('',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
            + "LIMIT 1000";
    String expected =
        "SELECT table_name AS quote_ident FROM information_schema.tables WHERE"
            + " table_schema = 'public' and STARTS_WITH(LOWER(table_name),"
            + " LOWER('user')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testTableInsertAutocomplete() {
    // PSQL equivalent: INSERT INTO <table>
    String sql =
        "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN"
            + " ('r', 'f', 'v', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,4)='user'"
            + " AND pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM"
            + " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4)='user' AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)"
            + " FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid"
            + " AND c.relkind IN ('r', 'f', 'v', 'p') AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.' ||"
            + " pg_catalog.quote_ident(c.relname),1,4)='user' AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND"
            + " (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
            + "LIMIT 1000";
    String expected =
        "SELECT table_name AS quote_ident FROM information_schema.tables WHERE"
            + " table_schema = 'public' and STARTS_WITH(LOWER(table_name),"
            + " LOWER('user')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testTableAttributesAutocomplete() {
    // PSQL equivalent: INSERT INTO table_name (<attribute>)
    // PSQL equivalent: SELECT * FROM table_name WHERE <attribute>
    String sql =
        "SELECT pg_catalog.quote_ident(attname)   FROM pg_catalog.pg_attribute a,"
            + " pg_catalog.pg_class c  WHERE c.oid = a.attrelid    AND a.attnum > 0    AND NOT"
            + " a.attisdropped    AND substring(pg_catalog.quote_ident(attname),1,3)='age'    AND"
            + " (pg_catalog.quote_ident(relname)='user'         OR '\"' || relname || '\"'='user') "
            + "   AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "LIMIT 1000";
    String expected =
        "SELECT column_name AS quote_ident FROM information_schema.columns WHERE"
            + " table_name = 'user' AND STARTS_WITH(LOWER(COLUMN_NAME), LOWER('age')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableAutocomplete() {
    // PSQL equivalent: \\d <table>
    String sql =
        "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE"
            + " substring(pg_catalog.quote_ident(c.relname),1,4)='user' AND"
            + " pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM"
            + " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4)='user' AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)"
            + " FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid"
            + " AND substring(pg_catalog.quote_ident(n.nspname) || '.' ||"
            + " pg_catalog.quote_ident(c.relname),1,4)='user' AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND"
            + " (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
            + "LIMIT 1000";
    String expected =
        "SELECT table_name AS quote_ident FROM information_schema.tables WHERE "
            + "table_schema = 'public' AND STARTS_WITH(LOWER(table_name), LOWER('user')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeTableMetadataAutocomplete() {
    // PSQL equivalent: \\dt <table>
    String sql =
        "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN"
            + " ('r', 'p') AND substring(pg_catalog.quote_ident(c.relname),1,4)='user' AND"
            + " pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM"
            + " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4)='user' AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)"
            + " FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid"
            + " AND c.relkind IN ('r', 'p') AND substring(pg_catalog.quote_ident(n.nspname) || '.'"
            + " || pg_catalog.quote_ident(c.relname),1,4)='user' AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND"
            + " (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,4) ="
            + " substring('user',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
            + "LIMIT 1000";
    String expected =
        "SELECT table_name AS quote_ident FROM INFORMATION_SCHEMA.TABLES WHERE"
            + " STARTS_WITH(LOWER(table_name), LOWER('user')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeIndexMetadataAutocomplete() {
    // PSQL equivalent: \\di <index>
    String sql =
        "SELECT pg_catalog.quote_ident(c.relname) FROM pg_catalog.pg_class c WHERE c.relkind IN"
            + " ('i', 'I') AND substring(pg_catalog.quote_ident(c.relname),1,5)='index' AND"
            + " pg_catalog.pg_table_is_visible(c.oid) AND c.relnamespace <> (SELECT oid FROM"
            + " pg_catalog.pg_namespace WHERE nspname = 'pg_catalog')\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' FROM pg_catalog.pg_namespace n WHERE"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,5)='index' AND (SELECT"
            + " pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,5) ="
            + " substring('index',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) > 1\n"
            + "UNION\n"
            + "SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)"
            + " FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n WHERE c.relnamespace = n.oid"
            + " AND c.relkind IN ('i', 'I') AND substring(pg_catalog.quote_ident(n.nspname) || '.'"
            + " || pg_catalog.quote_ident(c.relname),1,5)='index' AND"
            + " substring(pg_catalog.quote_ident(n.nspname) || '.',1,5) ="
            + " substring('index',1,pg_catalog.length(pg_catalog.quote_ident(n.nspname))+1) AND"
            + " (SELECT pg_catalog.count(*) FROM pg_catalog.pg_namespace WHERE"
            + " substring(pg_catalog.quote_ident(nspname) || '.',1,5) ="
            + " substring('index',1,pg_catalog.length(pg_catalog.quote_ident(nspname))+1)) = 1\n"
            + "LIMIT 1000";
    String expected =
        "SELECT index_name AS quote_ident FROM INFORMATION_SCHEMA.INDEXES WHERE"
            + " STARTS_WITH(LOWER(index_name), LOWER('index')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDescribeSchemaMetadataAutocomplete() {
    // PSQL equivalent: \\dn <schema>
    String sql =
        "SELECT pg_catalog.quote_ident(nspname) FROM pg_catalog.pg_namespace  WHERE"
            + " substring(pg_catalog.quote_ident(nspname),1,6)='schema'\n"
            + "LIMIT 1000";
    String expected =
        "SELECT schema_name AS quote_ident FROM INFORMATION_SCHEMA.SCHEMATA WHERE"
            + " STARTS_WITH(LOWER(schema_name), LOWER('schema')) LIMIT 1000";

    assertEquals(expected, translate(sql));
  }

  @Test
  public void testDynamicCommands() throws Exception {
    String inputJSON =
        ""
            + "{"
            + " \"commands\": "
            + "   [ "
            + "     {"
            + "       \"input_pattern\": \"^SELECT \\* FROM USERS;?$\", "
            + "       \"output_pattern\": \"RESULT 1\", "
            + "       \"matcher_array\": []"
            + "     },"
            + "     {"
            + "       \"input_pattern\": \"^SELECT (?<selector>.*) FROM USERS WHERE (?<arg1>.*) = (?<arg2>.*);?$\", "
            + "       \"output_pattern\": \"RESULT 2: selector=%s, arg2=%s, arg1=%s\", "
            + "       \"matcher_array\": [ \"selector\", \"arg2\", \"arg1\" ]"
            + "     }"
            + "   ]"
            + "}";

    JSONParser parser = new JSONParser();
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.getCommandMetadataJSON()).thenReturn((JSONObject) parser.parse(inputJSON));

    String firstSQL = "SELECT * FROM USERS;";
    String expectedFirstResult = "RESULT 1";
    String secondSQL = "SELECT name FROM USERS WHERE age = 30;";
    String expectedSecondResult = "RESULT 2: selector=name, arg2=30, arg1=age";

    assertEquals(expectedFirstResult, translate(firstSQL));
    assertEquals(expectedSecondResult, translate(secondSQL));
  }

  @Test
  public void testMatcherGroupInPlaceReplacements() throws Exception {
    String inputJSON =
        ""
            + "{"
            + " \"commands\": "
            + "   [ "
            + "     {"
            + "       \"input_pattern\": \"^SELECT (?<expression>.*) FROM (?<table>.*);?$\", "
            + "       \"output_pattern\": \"TABLE: ${table}, EXPRESSION: ${expression}\", "
            + "       \"matcher_array\": []"
            + "     }"
            + "   ]"
            + "}";

    JSONParser parser = new JSONParser();
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.getCommandMetadataJSON()).thenReturn((JSONObject) parser.parse(inputJSON));

    String sql = "SELECT * FROM USERS;";
    String expectedResult = "TABLE: USERS, EXPRESSION: *";

    assertEquals(expectedResult, translate(sql));
  }
}
