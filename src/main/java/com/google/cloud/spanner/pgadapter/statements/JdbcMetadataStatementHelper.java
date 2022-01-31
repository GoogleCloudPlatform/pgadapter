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
public class JdbcMetadataStatementHelper {

  /**
   * Indicates whether a SQL statement could potentially be a JDBC metadata query that needs
   * replacement.
   *
   * @param sql The query to check
   * @return true if the query could be a JDBC metadata query, and false if it definitely not.
   */
  static boolean isPotentialJdbcMetadataStatement(String sql) {
    // All JDBC metadata queries that need any replacements reference the pg_catalog schema.
    if (!sql.contains("pg_catalog.")) {
      return false;
    }
    // TODO: Register all known JDBC metadata query prefixes from all 42.x versions of the JDBC
    // driver and only return true if this query starts with one of those.
    return true;
  }

  static String replaceJdbcMetadtaStatement(String sql) {
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_PREFIX)
        || sql.startsWith(PgJdbcCatalog.PG_JDBC_EXPORTED_IMPORTED_KEYS_42_0_PREFIX)) {
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
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_GET_SCHEMAS_PREFIX)) {
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

    // Rewrite the 42.3.x PK query to the version in 42.2.x.
    if (sql.startsWith(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX_42_3)) {
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
      sql = replacedSql + whereClause + " ORDER BY ct.relname, pk_name, key_seq";
    }

    return sql
        // Replace fixed query prefixes.
        .replace(PgJdbcCatalog.PG_JDBC_PK_QUERY_PREFIX, PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)
        .replace(
            PgJdbcCatalog.PG_JDBC_BEST_ROW_IDENTIFIER_PREFIX,
            PgJdbcCatalog.PG_JDBC_PK_QUERY_REPLACEMENT)

        // Replace unsupported pg_catalog tables with fixed sub-selects.
        .replace(
            " JOIN pg_catalog.pg_description",
            String.format(" JOIN (%s)", PgJdbcCatalog.PG_DESCRIPTION))
        .replace(" pg_catalog.pg_type ", String.format(" (%s) ", PgJdbcCatalog.PG_TYPE))
        .replace(" pg_catalog.pg_am", String.format(" (%s)", PgJdbcCatalog.PG_AM))
        .replaceAll(
            "\\s+FROM\\s+pg_catalog\\.pg_settings",
            String.format(" FROM (%s) pg_settings", PgJdbcCatalog.PG_SETTINGS))

        // Add joins for tables that miss information.
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
}
