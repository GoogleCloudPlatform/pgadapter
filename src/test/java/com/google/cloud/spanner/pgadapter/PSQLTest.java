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

package com.google.cloud.spanner.pgadapter;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.statements.PSQLStatement;
import com.google.cloud.spanner.pgadapter.utils.StatementParser;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
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

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ConnectionHandler connectionHandler;

  @Mock
  private Connection connection;

  @Mock
  private ProxyServer server;

  @Mock
  private OptionsMetadata options;

  @Mock
  private Statement statement;


  @Before
  public void setup() throws Exception {
    JSONParser parser = new JSONParser();
    File commandMetadataFile = new File(OptionsMetadata.getDefaultCommandMetadataFilePath());

    Mockito.when(connectionHandler.getJdbcConnection()).thenReturn(connection);
    Mockito.when(connectionHandler.getServer()).thenReturn(server);
    Mockito.when(server.getOptions()).thenReturn(options);
    Mockito.when(options.getCommandMetadataJSON()).thenReturn(
        (JSONObject) parser.parse(new String(
            Files.readAllBytes(commandMetadataFile.toPath()))));
    Mockito.when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  public void testDescribeTranslates() throws SQLException {
    // PSQL equivalent: \d
    String sql =
        "SELECT n.nspname as \"Schema\",\n" +
            "  c.relname as \"Name\",\n" +
            "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
            +
            "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n" +
            "FROM pg_catalog.pg_class c\n" +
            "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
            "WHERE c.relkind IN ('r','p','v','m','S','f','')\n" +
            "      AND n.nspname <> 'pg_catalog'\n" +
            "      AND n.nspname <> 'information_schema'\n" +
            "      AND n.nspname !~ '^pg_toast'\n" +
            "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
            "ORDER BY 1,2;";
    String result =
        "SELECT" +
            " t.table_schema as Schema," +
            " t.table_name as Name," +
            " \"table\" as Type," +
            " \"me\" as Owner " +
            "FROM" +
            " information_schema.tables AS t " +
            "WHERE" +
            " t.table_schema = ''";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableMatchTranslates() throws SQLException {
    // PSQL equivalent: \d <table> (1)
    String sql =
        "SELECT c.oid,\n" +
            "  n.nspname,\n" +
            "  c.relname\n" +
            "FROM pg_catalog.pg_class c\n" +
            "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
            "WHERE c.relname OPERATOR(pg_catalog.~) '^(users)$'\n" +
            "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
            "ORDER BY 2, 3;";
    String result =
        "SELECT" +
            " farm_fingerprint(t.table_name) as oid," +
            " \"\" as nspname," +
            " t.table_name as relname" +
            " FROM" +
            " information_schema.tables AS t" +
            " WHERE" +
            " t.table_schema=''" +
            " AND" +
            " lower(t.table_name) = lower('users');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableMatchHandlesBobbyTables() throws SQLException {
    // PSQL equivalent: \d <table> (1)
    String sql =
        "SELECT c.oid,\n" +
            "  n.nspname,\n" +
            "  c.relname\n" +
            "FROM pg_catalog.pg_class c\n" +
            "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
            "WHERE c.relname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n" +
            "  AND pg_catalog.pg_table_is_visible(c.oid)\n" +
            "ORDER BY 2, 3;";
    String result =
        "SELECT" +
            " farm_fingerprint(t.table_name) as oid," +
            " \"\" as nspname," +
            " t.table_name as relname" +
            " FROM" +
            " information_schema.tables AS t" +
            " WHERE" +
            " t.table_schema=''" +
            " AND" +
            " lower(t.table_name) = lower('bobby\\'; DROP TABLE USERS; SELECT\\'');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableCatalogTranslates() throws SQLException {
    // PSQL equivalent: \d <table> (2)
    String sql =
        "SELECT relchecks, relkind, relhasindex, relhasrules, reltriggers <> 0, false, false, relhasoids, false as relispartition, '', ''\n"
            +
            "FROM pg_catalog.pg_class WHERE oid = '-2264987671676060158';";
    String result =
        "SELECT" +
            " 0 as relcheck," +
            " \"r\" as relkind," +
            " false as relhasindex," +
            " false as relhasrules," +
            " false as reltriggers," +
            " false," +
            " false," +
            " false as relhasoids," +
            " \"\"," +
            " \"\";";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableMetadataTranslates() throws SQLException {
    // PSQL equivalent: \d <table> (3)
    String sql =
        "SELECT a.attname,\n" +
            "  pg_catalog.format_type(a.atttypid, a.atttypmod),\n" +
            "  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)\n" +
            "   FROM pg_catalog.pg_attrdef d\n" +
            "   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n" +
            "  a.attnotnull,\n" +
            "  NULL AS attcollation,\n" +
            "  ''::pg_catalog.char AS attidentity,\n" +
            "  ''::pg_catalog.char AS attgenerated\n" +
            "FROM pg_catalog.pg_attribute a\n" +
            "WHERE a.attrelid = '-1' AND a.attnum > 0 AND NOT a.attisdropped\n" +
            "ORDER BY a.attnum;";
    String result =
        "SELECT" +
            " t.column_name as attname," +
            " t.spanner_type as format_type," +
            " \"\" as substring," +
            " t.is_nullable = \"NO\" as attnotnull," +
            " 1 as attnum," +
            " null as attcollation," +
            " null as indexdef," +
            " null as attfdwoptions" +
            " FROM" +
            " information_schema.columns AS t" +
            " WHERE" +
            " t.table_schema=''" +
            " AND farm_fingerprint(t.table_name) = -1;";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableMetadataHandlesBobbyTables() throws SQLException {
    // PSQL equivalent: \d <table> (3)
    String sql =
        "SELECT a.attname,\n" +
            "  pg_catalog.format_type(a.atttypid, a.atttypmod),\n" +
            "  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) for 128)\n" +
            "   FROM pg_catalog.pg_attrdef d\n" +
            "   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n" +
            "  a.attnotnull,\n" +
            "  NULL AS attcollation,\n" +
            "  ''::pg_catalog.char AS attidentity,\n" +
            "  ''::pg_catalog.char AS attgenerated\n" +
            "FROM pg_catalog.pg_attribute a\n" +
            "WHERE a.attrelid = 'bobby'; DROP TABLE USERS; SELECT'' AND a.attnum > 0 AND NOT a.attisdropped\n"
            +
            "ORDER BY a.attnum;";
    String result =
        "SELECT" +
            " t.column_name as attname," +
            " t.spanner_type as format_type," +
            " \"\" as substring," +
            " t.is_nullable = \"NO\" as attnotnull," +
            " 1 as attnum," +
            " null as attcollation," +
            " null as indexdef," +
            " null as attfdwoptions" +
            " FROM" +
            " information_schema.columns AS t" +
            " WHERE" +
            " t.table_schema=''" +
            " AND farm_fingerprint(t.table_name) = bobby\\'; DROP TABLE USERS; SELECT\\';";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeTableAttributesTranslates() throws SQLException {
    // PSQL equivalent: \d <table> (4)
    String sql =
        "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhparent AND i.inhrelid = '-2264987671676060158' AND c.relkind != 'p' ORDER BY inhseqno;\n";
    String result =
        "select 1 as oid from UNNEST([]);";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeMoreTableAttributesTranslates() throws SQLException {
    // PSQL equivalent: \d <table> (5)
    String sql =
        "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid=i.inhrelid AND i.inhparent = '-2264987671676060158' ORDER BY c.relname;\n";
    String result =
        "select 1 as oid from UNNEST([]);";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testListCommandTranslates() throws SQLException {
    // PSQL equivalent: \l
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "ORDER BY 1;";
    String result =
        "SELECT 'users' AS Name;";

    Mockito.when(connection.getCatalog()).thenReturn("users");

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testListDatabaseCommandTranslates() throws SQLException {
    // PSQL equivalent: \l <table>
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "WHERE d.datname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "ORDER BY 1;";
    String result =
        "SELECT 'users' AS Name;";

    Mockito.when(connection.getCatalog()).thenReturn("users");

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testListDatabaseCommandFailsButPrintsUnknown() throws SQLException {
    // PSQL equivalent: \l <table>
    String sql =
        "SELECT d.datname as \"Name\",\n"
            + "       pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\",\n"
            + "       pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\",\n"
            + "       pg_catalog.array_to_string(d.datacl, '\\n') AS \"Access privileges\"\n"
            + "FROM pg_catalog.pg_database d\n"
            + "WHERE d.datname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "ORDER BY 1;";
    String result =
        "SELECT 'UNKNOWN' AS Name;";

    Mockito.when(connection.getCatalog()).thenThrow(SQLException.class);

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeAllTableMetadataTranslates() throws SQLException {
    // PSQL equivalent: \dt
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','')\n"
            + "      AND n.nspname <> 'pg_catalog'\n"
            + "      AND n.nspname <> 'information_schema'\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES;";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedTableMetadataTranslates() throws SQLException {
    // PSQL equivalent: \dt <table>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(users)$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE lower(TABLE_NAME) = lower('users');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedTableMetadataHandlesBobbyTables() throws SQLException {
    // PSQL equivalent: \dt <table>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
            + "  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n"
            + "FROM pg_catalog.pg_class c\n"
            + "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n"
            + "WHERE c.relkind IN ('r','p','s','')\n"
            + "      AND n.nspname !~ '^pg_toast'\n"
            + "  AND c.relname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "  AND pg_catalog.pg_table_is_visible(c.oid)\n"
            + "ORDER BY 1,2;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE lower(TABLE_NAME) = lower('bobby\\'; DROP TABLE USERS; SELECT\\'');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeAllIndexMetadataTranslates() throws SQLException {
    // PSQL equivalent: \di
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
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
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.INDEXES";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedIndexMetadataTranslates() throws SQLException {
    // PSQL equivalent: \di <index>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
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
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE lower(INDEX_NAME) = lower('index');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedIndexMetadataHandlesBobbyTables() throws SQLException {
    // PSQL equivalent: \di <index>
    String sql =
        "SELECT n.nspname as \"Schema\",\n"
            + "  c.relname as \"Name\",\n"
            + "  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\",\n"
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
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE lower(INDEX_NAME) = lower('bobby\\'; DROP TABLE USERS; SELECT\\'');";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeAllSchemaMetadataTranslates() throws SQLException {
    // PSQL equivalent: \dn
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'\n"
            + "ORDER BY 1;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedSchemaMetadataTranslates() throws SQLException {
    // PSQL equivalent: \dn <schema>
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname OPERATOR(pg_catalog.~) '^(schema)$'\n"
            + "ORDER BY 1;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE lower(SCHEMA_NAME) = lower('schema')";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDescribeSelectedSchemaMetadataHandlesBobbyTables() throws SQLException {
    // PSQL equivalent: \dn <schema>
    String sql =
        "SELECT n.nspname AS \"Name\",\n"
            + "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n"
            + "FROM pg_catalog.pg_namespace n\n"
            + "WHERE n.nspname OPERATOR(pg_catalog.~) '^(bobby'; DROP TABLE USERS; SELECT')$'\n"
            + "ORDER BY 1;";
    String result =
        "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE lower(SCHEMA_NAME) = lower('bobby\\'; DROP TABLE USERS; SELECT\\'')";

    PSQLStatement psqlStatement = new PSQLStatement(sql, connectionHandler);

    Assert.assertEquals(psqlStatement.getSql(), result);
  }

  @Test
  public void testDynamicCommands() throws Exception {
    String inputJSON = ""
        + "{"
        + " \"commands\": "
        + "   [ "
        + "     {"
        + "       \"input_pattern\": \"^SELECT \\* FROM USERS;$\", "
        + "       \"output_pattern\": \"RESULT 1\", "
        + "       \"matcher_array\": []"
        + "     },"
        + "     {"
        + "       \"input_pattern\": \"^SELECT (?<selector>.*) FROM USERS WHERE (?<arg1>.*) = (?<arg2>.*);$\", "
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

    PSQLStatement psqlStatement = new PSQLStatement(firstSQL, connectionHandler);
    Assert.assertEquals(psqlStatement.getSql(), expectedFirstResult);

    psqlStatement = new PSQLStatement(secondSQL, connectionHandler);
    Assert.assertEquals(psqlStatement.getSql(), expectedSecondResult);
  }

  @Test
  public void testEscapes() {
    String sql = "Bobby\\'O\\'Bob'; DROP TABLE USERS; select'";
    String expectedSql = "Bobby\\'O\\'Bob\\'; DROP TABLE USERS; select\\'";
    Assert.assertEquals(
        StatementParser.singleQuoteEscape(sql),
        expectedSql);
  }
}
