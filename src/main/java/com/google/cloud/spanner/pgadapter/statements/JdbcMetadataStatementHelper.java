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

import com.google.cloud.spanner.pgadapter.utils.PgJdbcCatalog;

/**
 * Helper class for identifying and replacing JDBC metadata statements with statements that are
 * supported by Spangres. Spangres currently has limited support for many pg_catalog tables, and
 * also does not support all query constructs that are used by JDBC metadata statements.
 *
 * <p>This class detects whether a statement is generated by a JDBC metadata method and replaces it
 * with a compatible query.
 */
class JdbcMetadataStatementHelper {

  /**
   * Indicates whether a SQL statement could potentially be a JDBC metadata query that needs
   * replacement.
   *
   * @param sql The query to check
   * @return true if the query could be a JDBC metadata query, and false if it definitely not.
   */
  static boolean isPotentialJdbcMetadataStatement(String sql) {
    // All JDBC metadata queries that need any replacements reference the pg_catalog schema or the
    // pg_settings table.
    return sql.contains("pg_catalog.") || sql.contains("pg_settings");
  }

  static String replaceJdbcMetadataStatement(String sql) {
    // First we look for a number of fixed query prefixes for queries that are completely replaced
    // with Spangres-compatible queries.
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_PREFIX)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_42_0_PREFIX)) {
      return replaceImportedExportedKeysQuery(sql);
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_SCHEMAS_PREFIX)) {
      return replaceGetSchemasQuery(sql);
    }
    if (sql.equals(PgJdbcCatalog.PG_JDBC_GET_EDB_REDWOOD_DATE_QUERY)) {
      return PgJdbcCatalog.PG_JDBC_GET_EDB_REDWOOD_DATE_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_1)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_2)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_3)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_4)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_5)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_6)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_7)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLES_PREFIX_8)) {
      return replaceGetTablesQuery(sql);
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_COLUMNS_PREFIX_1)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_COLUMNS_PREFIX_2)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_COLUMNS_PREFIX_3)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_COLUMNS_PREFIX_4)) {
      return replaceGetColumnsQuery(sql);
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_INDEXES_PREFIX_1)) {
      return replaceGetIndexInfoQuery(sql);
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_PRIMARY_KEY_PREFIX_1)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_PRIMARY_KEY_PREFIX_2)) {
      return replaceGetPrimaryKeyQuery(sql);
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX_FULL)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_FULL;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITH_TYPTYPE_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITH_TYPTYPE_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITH_PARAMETER_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITH_PARAMETER_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PARAM_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PARAM_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_NOT_TOAST_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_NOT_TOAST_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLE_PRIVILEGES_PREFIX_1)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TABLE_PRIVILEGES_PREFIX_2)) {
      return PgJdbcCatalog.PG_JDBC_GET_TABLE_PRIVILEGES_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_COLUMN_PRIVILEGES_PREFIX_1)) {
      return PgJdbcCatalog.PG_JDBC_GET_TABLE_PRIVILEGES_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_BEST_ROW_IDENTIFIER_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_BEST_ROW_IDENTIFIER_REPLACEMENT;
    }

    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_WITH_FUNC_TYPE_PREFIX)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_WITHOUT_FUNC_TYPE_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_FUNCTIONS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_FUNCTION_COLUMNS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_FUNCTION_COLUMNS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_PROCEDURES_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_PROCEDURES_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_UDTS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_UDTS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_MAX_NAME_LENGTH_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_MAX_NAME_LENGTH_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_SQL_KEYWORDS_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_SQL_KEYWORDS_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX_42_3)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_PREFIX_42_2_22)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_SIMPLE_PREFIX_42_3)) {
      return PgJdbcCatalog.PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3;
    }

    // The query that is used to fetch the primary keys of a table was re-written when the PG driver
    // went from 42.2.x to 42.3.x. We translate the 42.3.x query back to the 42.2.x version here,
    // so we can use the same query replacement for both versions.
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX_42_3)) {
      sql = replacePG42_3_PkQuery_With_42_2_Query(sql);
    }

    // Replace fixed query prefixes and known unsupported tables.
    return sql
        // Replace fixed query prefixes.
        .replace(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX, PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)
        .replace(
            PgJdbcCatalog.PG_JDBC_BEST_ROW_IDENTIFIER_PREFIX,
            PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)

        // Replace unsupported pg_catalog tables with fixed (empty) sub-selects.
        .replace(
            " JOIN pg_catalog.pg_description",
            String.format(" JOIN (%s)", PgJdbcCatalog.PG_DESCRIPTION))
        .replace(" pg_catalog.pg_type ", String.format(" (%s) ", PgJdbcCatalog.PG_TYPE))
        .replace(" pg_catalog.pg_am", String.format(" (%s)", PgJdbcCatalog.PG_AM))
        .replaceAll(
            "\\s+FROM\\s+pg_catalog\\.pg_settings",
            String.format(" FROM (%s) pg_settings", PgJdbcCatalog.PG_SETTINGS))

        // Add joins for tables that miss information to get the required information.
        .replace(
            " JOIN pg_catalog.pg_attribute a",
            String.format(
                " JOIN pg_catalog.pg_attribute a INNER JOIN (%s) a_spanner ON a.attrelid=a_spanner.attrelid AND a.attname=a_spanner.attname",
                PgJdbcCatalog.PG_ATTR_TYPE))

        // Replace select list expressions for those that might miss information.
        .replace(",a.atttypid,", ",coalesce(a.atttypid, a_spanner.spanner_atttypid) as atttypid,")

        // Replace expressions known to be in JDBC metadata queries that are not supported.
        // Replace !~ with NOT <expr> ~
        .replace("AND n.nspname !~ '^pg_'", "AND NOT n.nspname ~ '^pg_'")

        // Replace where and join conditions known to be in JDBC metadata queries that need
        // replacement.
        .replace("AND ci.relam=am.oid", "AND coalesce(ci.relam, 1)=am.oid")
        .replace(
            " ON (a.atttypid = t.oid)",
            " ON (coalesce(a.atttypid, a_spanner.spanner_atttypid) = t.oid)")
        .replace(
            " OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)",
            " OR FALSE ")

        // Replace function calls for DDL definitions with null literals.
        .replaceAll("pg_catalog\\.pg_get_expr\\(.+?\\)", "null")
        .replaceAll("pg_catalog\\.pg_get_indexdef\\(.+?\\)", "null")

        // Replace unsupported casts.
        .replaceAll("'pg_class'::regclass", "0");
  }

  /**
   * Replaces the getPrimaryKeys query in PG JDBC 42.3.x with the one in 42.2.x, so we can base the
   * further query replacement on the 42.2.x version.
   */
  private static String replacePG42_3_PkQuery_With_42_2_Query(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX;
    String whereClause = "";
    if (sql.contains(" AND n.nspname = ")) {
      int startIndex = sql.indexOf(" AND n.nspname = ");
      int endIndex = sql.indexOf(" AND ct.relname = ", startIndex);
      whereClause += sql.substring(startIndex, endIndex);
    }
    if (sql.contains(" AND ct.relname = ")) {
      int startIndex = sql.indexOf(" AND ct.relname = ");
      int endIndex = sql.indexOf(" AND i.indisprimary ", startIndex);
      whereClause += sql.substring(startIndex, endIndex);
    }
    return replacedSql + whereClause + " ORDER BY ct.relname, pk_name, key_seq";
  }

  private static String replaceGetSchemasQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_GET_SCHEMAS_REPLACEMENT;
    int startIndex;
    if (sql.contains(" AND nspname LIKE ")) {
      startIndex = sql.indexOf(" AND nspname LIKE ");
    } else {
      startIndex = sql.indexOf(" ORDER BY ");
    }
    replacedSql += sql.substring(startIndex);
    return replacedSql
        .replace(" AND nspname LIKE ", " AND schema_name LIKE ")
        .replace(" ORDER BY TABLE_SCHEM", " ORDER BY schema_name");
  }

  private static String replaceGetTablesQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_GET_TABLES_REPLACEMENT;
    int startIndex;
    String wherePrefix = "WHERE c.relnamespace = n.oid";
    if (sql.contains(wherePrefix)) {
      startIndex = sql.indexOf(wherePrefix) + wherePrefix.length();
    } else {
      return sql;
    }
    replacedSql += " WHERE TRUE " + sql.substring(startIndex);
    return replacedSql
        .replace(" AND n.nspname LIKE ", " AND TABLE_SCHEMA LIKE ")
        .replace(" AND c.relname LIKE ", " AND TABLE_NAME LIKE ")
        .replace(
            "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TABLE'")
        .replace(
            "c.relkind IN ('r','p') AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TABLE'")
        .replace(
            "c.relkind = 'p' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TABLE'")
        .replace(
            "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'VIEW'")
        .replace(
            "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'INDEX'")
        .replace(
            "c.relkind = 'I' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'INDEX'")
        .replace(
            "c.relkind = 'S' AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP SEQUENCE'")
        .replace(
            "c.relkind = 'S'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SEQUENCE'")
        .replace(
            "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TYPE'")
        .replace(
            "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SYSTEM TABLE'")
        .replace(
            "c.relkind = 'r' AND n.nspname = 'pg_toast'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SYSTEM TOAST TABLE'")
        .replace(
            "c.relkind = 'i' AND n.nspname = 'pg_toast'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SYSTEM TOAST INDEX'")
        .replace(
            "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SYSTEM VIEW'")
        .replace(
            "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'SYSTEM INDEX'")
        .replace(
            "c.relkind IN ('r','p') AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP TABLE'")
        .replace(
            "c.relkind = 'r' AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP TABLE'")
        .replace(
            "c.relkind = 'i' AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP INDEX'")
        .replace(
            "c.relkind = 'v' AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP VIEW'")
        .replace(
            "c.relkind = 'S' AND n.nspname ~ '^pg_temp_'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'TEMP SEQUENCE'")
        .replace(
            "c.relkind = 'f'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'FOREIGN TABLE'")
        .replace(
            "c.relkind = 'm'",
            "(CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END) = 'MATERIALIZED VIEW'")
        .replace(
            "ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME",
            "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME");
  }

  private static String replaceGetColumnsQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_GET_COLUMNS_REPLACEMENT;
    int startIndex;
    String wherePrefix1 =
        " WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped ";
    String wherePrefix2 =
        " WHERE c.relkind in ('r','v','f','m') and a.attnum > 0 AND NOT a.attisdropped ";
    if (sql.contains(wherePrefix1)) {
      startIndex = sql.indexOf(wherePrefix1) + wherePrefix1.length();
    } else if (sql.contains(wherePrefix2)) {
      startIndex = sql.indexOf(wherePrefix2) + wherePrefix2.length();
    } else {
      return sql;
    }
    replacedSql += " WHERE TRUE " + sql.substring(startIndex);
    return replacedSql
        .replace(" AND n.nspname LIKE ", " AND TABLE_SCHEMA LIKE ")
        .replace(" AND c.relname LIKE ", " AND TABLE_NAME LIKE ")
        .replace(" AND attname LIKE ", " AND COLUMN_NAME LIKE ")
        .replace(
            "ORDER BY nspname,c.relname,attnum",
            "ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
  }

  private static String replaceGetIndexInfoQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_GET_INDEXES_REPLACEMENT;
    int startIndex;
    String wherePrefix1 =
        " WHERE ct.oid=i.indrelid AND ci.oid=i.indexrelid AND a.attrelid=ci.oid AND ci.relam=am.oid  AND n.oid = ct.relnamespace ";
    if (sql.contains(wherePrefix1)) {
      startIndex = sql.indexOf(wherePrefix1) + wherePrefix1.length();
    } else {
      return sql;
    }
    replacedSql += " WHERE TRUE " + sql.substring(startIndex);
    return replacedSql
        .replace(" AND n.nspname = ", " AND IDX.TABLE_SCHEMA = ")
        .replace(" AND ct.relname = ", " AND IDX.TABLE_NAME = ")
        .replace(" AND i.indisunique ", " AND IDX.IS_UNIQUE='YES' ")
        .replace(
            "ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION",
            "ORDER BY IDX.TABLE_NAME, IS_UNIQUE DESC, IDX.INDEX_NAME, CASE WHEN ORDINAL_POSITION IS NULL THEN 0 ELSE ORDINAL_POSITION END");
  }

  private static String replaceGetPrimaryKeyQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_GET_PRIMARY_KEY_REPLACEMENT;
    int startIndex;
    String wherePrefix1 = " WHERE true ";
    if (sql.contains(wherePrefix1)) {
      startIndex = sql.indexOf(wherePrefix1) + wherePrefix1.length();
    } else {
      return sql;
    }
    replacedSql += " WHERE TRUE " + sql.substring(startIndex);
    return replacedSql
        .replace(") result where  result.A_ATTNUM = (result.KEYS).x", "")
        .replace(" AND n.nspname = ", " AND IDX.TABLE_SCHEMA = ")
        .replace(" AND ct.relname = ", " AND IDX.TABLE_NAME = ")
        .replace(" AND i.indisprimary ", " AND IDX.INDEX_TYPE='PRIMARY_KEY' ")
        .replace(
            "ORDER BY result.table_name, result.pk_name, result.key_seq",
            "ORDER BY COLS.TABLE_NAME, IDX.INDEX_NAME, COLS.ORDINAL_POSITION")
        .replace(
            "ORDER BY table_name, pk_name, key_seq",
            "ORDER BY COLS.TABLE_NAME, IDX.INDEX_NAME, COLS.ORDINAL_POSITION");
  }

  private static String replaceImportedExportedKeysQuery(String sql) {
    String replacedSql = PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_REPLACEMENT;
    int startIndex;
    if (sql.contains(" AND pkn.nspname = ")) {
      startIndex = sql.indexOf(" AND pkn.nspname = ");
    } else if (sql.contains(" AND fkn.nspname = ")) {
      startIndex = sql.indexOf(" AND fkn.nspname = ");
    } else if (sql.contains(" AND pkc.relname = ")) {
      startIndex = sql.indexOf(" AND pkc.relname = ");
    } else if (sql.contains(" AND fkc.relname = ")) {
      startIndex = sql.indexOf(" AND fkc.relname = ");
    } else {
      startIndex = sql.indexOf(" ORDER BY ");
    }
    replacedSql += sql.substring(startIndex);
    return replacedSql
        .replace(" AND pkn.nspname = ", " AND PARENT.TABLE_SCHEMA = ")
        .replace(" AND fkn.nspname = ", " AND CHILD.TABLE_SCHEMA = ")
        .replace(" AND pkc.relname = ", " AND PARENT.TABLE_NAME = ")
        .replace(" AND fkc.relname = ", " AND CHILD.TABLE_NAME = ")
        .replace(
            " ORDER BY fkn.nspname,fkc.relname,con.conname,pos.n",
            " ORDER BY CHILD.TABLE_CATALOG, CHILD.TABLE_SCHEMA, CHILD.TABLE_NAME, KEY_SEQ")
        .replace(
            " ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n",
            " ORDER BY PARENT.TABLE_CATALOG, PARENT.TABLE_SCHEMA, PARENT.TABLE_NAME, KEY_SEQ");
  }
}
