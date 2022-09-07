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

package com.google.cloud.spanner.pgadapter.utils;

import com.google.api.core.InternalApi;
import java.sql.DatabaseMetaData;

/** Contains replacements for known queries that are executed by the PG JDBC driver. */
@InternalApi
public class PgJdbcCatalog {
  public static final String PG_JDBC_GET_MAX_NAME_LENGTH_PREFIX =
      "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n "
          + "WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'";
  public static final String PG_JDBC_GET_MAX_NAME_LENGTH_REPLACEMENT =
      "SELECT 129::bigint as typlen";

  public static final String PG_JDBC_GET_SQL_KEYWORDS_PREFIX =
      "select string_agg(word, ',') from pg_catalog.pg_get_keywords() " + "where word <> ALL";
  public static final String PG_JDBC_GET_SQL_KEYWORDS_REPLACEMENT =
      "select 'abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,"
          + "cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,"
          + "delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,"
          + "greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,"
          + "instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,"
          + "nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,"
          + "recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,"
          + "statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,"
          + "unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile'";

  public static final String PG_JDBC_GET_TYPE_NAME =
      "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname "
          + "FROM pg_catalog.pg_type t "
          + "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1";
  public static final String PG_JDBC_GET_TYPE_NAME_REPLACEMENT =
      "SELECT true, n.nspname, t.typname "
          + "FROM pg_catalog.pg_type t "
          + "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1";
  public static final String PG_JDBC_GET_TYPE_INFO_PREFIX_42_2_22 =
      "SELECT typinput='array_in'::regproc as is_array, typtype, typname "
          + "  FROM pg_catalog.pg_type "
          + "  LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r "
          + "          from pg_namespace as ns "
          + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
          + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
          + "         using ( nspname ) "
          + "       ) as sp "
          + "    ON sp.nspoid = typnamespace ";
  public static final String PG_JDBC_GET_TYPE_INFO_PREFIX_42_3 =
      "SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type.oid "
          + "  FROM pg_catalog.pg_type "
          + "  LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r "
          + "          from pg_namespace as ns "
          + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
          + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
          + "         using ( nspname ) "
          + "       ) as sp "
          + "    ON sp.nspoid = typnamespace ";
  public static final String PG_JDBC_GET_TYPE_INFO_SIMPLE_PREFIX_42_3 =
      "SELECT pg_type.oid, typname "
          + "  FROM pg_catalog.pg_type "
          + "  LEFT "
          + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
          + "          from pg_namespace as ns "
          + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
          + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
          + "         using ( nspname ) "
          + "       ) as sp "
          + "    ON sp.nspoid = typnamespace ";
  public static final String PG_JDBC_GET_TYPE_INFO_REPLACEMENT_42_3 =
      "SELECT * FROM (SELECT ''::varchar as typinput, 0::bigint as typtype, ''::varchar as typname, 0::bigint as oid) pg_type WHERE false";

  public static final String PG_JDBC_GET_TYPE_INFO_PREFIX =
      "SELECT typinput='array_in'::regproc, typtype "
          + "  FROM pg_catalog.pg_type "
          + "  LEFT "
          + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
          + "          from pg_namespace as ns "
          + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
          + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
          + "         using ( nspname ) "
          + "       ) as sp "
          + "    ON sp.nspoid = typnamespace ";
  public static final String PG_JDBC_GET_TYPE_INFO_REPLACEMENT =
      "SELECT * FROM (SELECT ''::varchar as typinput, 0::bigint as typtype) pg_type WHERE false";

  private static final String PG_JDBC_FUNC_TYPE_UNKNOWN_SQL =
      DatabaseMetaData.functionResultUnknown + " ";
  private static final String PG_JDBC_FUNC_TYPE_SQL =
      " CASE "
          + "   WHEN (format_type(p.prorettype, null) = 'unknown') THEN "
          + DatabaseMetaData.functionResultUnknown
          + "   WHEN "
          + "     (substring(pg_get_function_result(p.oid) from 0 for 6) = 'TABLE') OR "
          + "     (substring(pg_get_function_result(p.oid) from 0 for 6) = 'SETOF') THEN "
          + DatabaseMetaData.functionReturnsTable
          + "   ELSE "
          + DatabaseMetaData.functionNoTable
          + " END ";
  public static final String PG_JDBC_GET_FUNCTIONS_WITHOUT_FUNC_TYPE_PREFIX =
      "SELECT current_database() AS FUNCTION_CAT, n.nspname AS FUNCTION_SCHEM, p.proname AS FUNCTION_NAME, "
          + " d.description AS REMARKS, "
          + PG_JDBC_FUNC_TYPE_UNKNOWN_SQL
          + " AS FUNCTION_TYPE, "
          + " p.proname || '_' || p.oid AS SPECIFIC_NAME "
          + "FROM pg_catalog.pg_proc p "
          + "INNER JOIN pg_catalog.pg_namespace n ON p.pronamespace=n.oid "
          + "LEFT JOIN pg_catalog.pg_description d ON p.oid=d.objoid ";
  public static final String PG_JDBC_GET_FUNCTIONS_WITH_FUNC_TYPE_PREFIX =
      "SELECT current_database() AS FUNCTION_CAT, n.nspname AS FUNCTION_SCHEM, p.proname AS FUNCTION_NAME, "
          + " d.description AS REMARKS, "
          + PG_JDBC_FUNC_TYPE_SQL
          + " AS FUNCTION_TYPE, "
          + " p.proname || '_' || p.oid AS SPECIFIC_NAME "
          + "FROM pg_catalog.pg_proc p "
          + "INNER JOIN pg_catalog.pg_namespace n ON p.pronamespace=n.oid "
          + "LEFT JOIN pg_catalog.pg_description d ON p.oid=d.objoid ";
  public static final String PG_JDBC_GET_FUNCTIONS_REPLACEMENT =
      "select * from (\n"
          + "\tselect ''::varchar as FUNCTION_CAT, ''::varchar as FUNCTION_SCHEM, ''::varchar as FUNCTION_NAME, ''::varchar as REMARKS, 0::bigint as FUNCTION_TYPE, ''::varchar as SPECIFIC_NAME\n"
          + ") f\n"
          + "where false";
  public static final String PG_JDBC_GET_PROCEDURES_PREFIX =
      "SELECT NULL AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, "
          + "NULL, NULL, NULL, d.description AS REMARKS, "
          + DatabaseMetaData.procedureReturnsResult
          + " AS PROCEDURE_TYPE, "
          + " p.proname || '_' || p.oid AS SPECIFIC_NAME "
          + " FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc p "
          + " LEFT JOIN pg_catalog.pg_description d ON (p.oid=d.objoid) "
          + " LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') "
          + " LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') "
          + " WHERE p.pronamespace=n.oid ";
  public static final String PG_JDBC_GET_PROCEDURES_REPLACEMENT =
      "select * from (\n"
          + "\tselect ''::varchar as PROCEDURE_CAT, ''::varchar as PROCEDURE_SCHEM, ''::varchar as PROCEDURE_NAME, ''::varchar as REMARKS, 0::bigint as PROCEDURE_TYPE, ''::varchar as SPECIFIC_NAME\n"
          + ") f\n"
          + "where false";

  public static final String PG_JDBC_GET_UDTS_PREFIX =
      "select "
          + "null as type_cat, n.nspname as type_schem, t.typname as type_name,  null as class_name, "
          + "CASE WHEN t.typtype='c' then "
          + java.sql.Types.STRUCT
          + " else "
          + java.sql.Types.DISTINCT
          + " end as data_type, pg_catalog.obj_description(t.oid, 'pg_type')  "
          + "as remarks, CASE WHEN t.typtype = 'd' then  (select CASE";
  public static final String PG_JDBC_GET_UDTS_REPLACEMENT =
      "select * from (\n"
          + "\tselect ''::varchar as TYPE_CAT, ''::varchar as TYPE_SCHEM, ''::varchar as TYPE_NAME, ''::varchar as CLASS_NAME, 0::bigint as DATA_TYPE, ''::varchar as REMARKS, 0::bigint as BASE_TYPE\n"
          + ") f\n"
          + "where false";

  public static final String PG_JDBC_GET_FUNCTION_COLUMNS_PREFIX =
      "SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid, "
          + " p.proargnames, p.proargmodes, p.proallargtypes, p.oid "
          + " FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t "
          + " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid ";
  public static final String PG_JDBC_GET_FUNCTION_COLUMNS_REPLACEMENT =
      "select * from (\n"
          + "\tselect ''::varchar as FUNCTION_CAT, ''::varchar as FUNCTION_SCHEM, ''::varchar as FUNCTION_NAME, ''::varchar as COLUMN_NAME, 0::bigint as COLUMN_TYPE,\n"
          + "\t0::bigint as DATA_TYPE, ''::varchar as TYPE_NAME, 0::bigint as PRECISION, 0::bigint as LENGTH, 0::bigint as SCALE, 0::bigint as RADIX, 0::bigint as NULLABLE,\n"
          + "\t''::varchar as REMARKS, 0::bigint as CHAR_OCTET_LENGTH, 0::bigint as ORDINAL_POSITION, ''::varchar as IS_NULLABLE, ''::varchar as SPECIFIC_NAME\n"
          + ") f\n"
          + "where false";

  public static final String PG_JDBC_GET_EDB_REDWOOD_DATE_QUERY =
      "select setting from pg_settings where name = 'edb_redwood_date'";
  public static final String PG_JDBC_GET_EDB_REDWOOD_DATE_REPLACEMENT =
      "select * from (select ''::varchar as setting) s where false";

  public static final String PG_JDBC_GET_SCHEMAS_PREFIX =
      "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace "
          + " WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_' "
          + " OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_' "
          + " OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) ";
  public static final String PG_JDBC_GET_SCHEMAS_REPLACEMENT =
      "select schema_name as TABLE_SCHEM, catalog_name AS TABLE_CATALOG from information_schema.schemata WHERE true ";

  public static final String PG_JDBC_GET_TABLES_PREFIX_1 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN (select * from (select 0 as objoid, 0 as classoid, 0 as objsubid, '' as description) pg_description where false) d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 0)  WHERE c.relnamespace = n.oid";
  public static final String PG_JDBC_GET_TABLES_PREFIX_2 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_3 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN (select * from (select 0 as objoid, 0 as classoid, 0 as objsubid, '' as description) pg_description where false) d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_4 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_5 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_6 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_7 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_PREFIX_8 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  ";
  public static final String PG_JDBC_GET_TABLES_REPLACEMENT =
      "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME AS TABLE_NAME,\n"
          + "       CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS TABLE_TYPE,\n"
          + "       NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME,\n"
          + "       NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION\n"
          + "FROM INFORMATION_SCHEMA.TABLES AS T\n";

  public static final String PG_JDBC_GET_COLUMNS_PREFIX_1 =
      "SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,a.attnum,null as attidentity,null as attgenerated,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')";
  public static final String PG_JDBC_GET_COLUMNS_PREFIX_2 =
      "SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,a.attnum,null as attidentity,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')";
  public static final String PG_JDBC_GET_COLUMNS_PREFIX_3 =
      "SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,a.attnum,null as attidentity,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')";
  public static final String PG_JDBC_GET_COLUMNS_PREFIX_4 =
      "SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,a.attnum,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')";
  public static final String PG_JDBC_GET_COLUMNS_REPLACEMENT =
      "SELECT TABLE_CATALOG AS \"TABLE_CAT\", TABLE_SCHEMA AS nspname, TABLE_NAME AS relname, COLUMN_NAME AS attname, '' as typtype,\n"
          + "       CASE\n"
          + "           WHEN DATA_TYPE = 'boolean' THEN 16\n"
          + "           WHEN DATA_TYPE = 'boolean[]' THEN 1000\n"
          + "           WHEN DATA_TYPE = 'bytea' THEN 17\n"
          + "           WHEN DATA_TYPE = 'bytea[]' THEN 1001\n"
          + "           WHEN DATA_TYPE = 'date' THEN 1082\n"
          + "           WHEN DATA_TYPE = 'date[]' THEN 1182\n"
          + "           WHEN DATA_TYPE = 'double precision' THEN 701\n"
          + "           WHEN DATA_TYPE = 'double precision[]' THEN 1022\n"
          + "           WHEN DATA_TYPE = 'bigint' THEN 20\n"
          + "           WHEN DATA_TYPE = 'bigint[]' THEN 1016\n"
          + "           WHEN DATA_TYPE = 'numeric' THEN 1700\n"
          + "           WHEN DATA_TYPE = 'numeric[]' THEN 1231\n"
          + "           WHEN DATA_TYPE = 'character varying' THEN 1043\n"
          + "           WHEN DATA_TYPE = 'character varying[]' THEN 1015\n"
          + "           WHEN DATA_TYPE = 'jsonb' THEN 3802\n"
          + "           WHEN DATA_TYPE = 'jsonb[]' THEN 3807\n"
          + "           WHEN DATA_TYPE = 'timestamp with time zone' THEN 1184\n"
          + "           WHEN DATA_TYPE = 'timestamp with time zone[]' THEN 1185\n"
          + "           END AS atttypid,\n"
          + "       DATA_TYPE AS atttypid, -1 as atttypmod,\n"
          + "       CASE\n"
          + "           WHEN DATA_TYPE LIKE 'ARRAY' THEN 0\n"
          + "           WHEN DATA_TYPE = 'boolean' THEN NULL\n"
          + "           WHEN DATA_TYPE LIKE 'bytea' THEN 10485760\n"
          + "           WHEN DATA_TYPE = 'date' THEN 10\n"
          + "           WHEN DATA_TYPE = 'double precision' THEN 15\n"
          + "           WHEN DATA_TYPE = 'bigint' THEN 19\n"
          + "           WHEN DATA_TYPE = 'numeric' THEN 15\n"
          + "           WHEN DATA_TYPE LIKE 'character varying' THEN CHARACTER_MAXIMUM_LENGTH\n"
          + "           WHEN DATA_TYPE = 'jsonb' THEN 2621440\n"
          + "           WHEN DATA_TYPE = 'timestamp with time zone' THEN 35\n"
          + "           END AS \"COLUMN_SIZE\",\n"
          + "       0 AS \"BUFFER_LENGTH\",\n"
          + "       CASE\n"
          + "           WHEN DATA_TYPE LIKE 'double precision' THEN 16\n"
          + "           WHEN DATA_TYPE LIKE 'numeric' THEN 16383\n"
          + "           ELSE NULL\n"
          + "           END AS \"DECIMAL_DIGITS\",\n"
          + "       CASE\n"
          + "           WHEN DATA_TYPE LIKE 'bigint' THEN 10\n"
          + "           WHEN DATA_TYPE LIKE 'numeric' THEN 10\n"
          + "           WHEN DATA_TYPE LIKE 'double precision' THEN 2\n"
          + "           ELSE NULL\n"
          + "           END AS \"NUM_PREC_RADIX\",\n"
          + "       CASE\n"
          + "           WHEN IS_NULLABLE = 'YES' THEN TRUE\n"
          + "           WHEN IS_NULLABLE = 'NO' THEN FALSE\n"
          + "           ELSE FALSE\n"
          + "           END AS attnotnull,\n"
          + "       NULL AS description,\n"
          + "       column_default AS adsrc,\n"
          + "       NULL AS attidentity,\n"
          + "       NULL AS attgenerated,\n"
          + "       NULL AS typbasetype,\n"
          + "       0 AS \"SQL_DATA_TYPE\",\n"
          + "       0 AS \"SQL_DATETIME_SUB\",\n"
          + "       CHARACTER_MAXIMUM_LENGTH AS \"CHAR_OCTET_LENGTH\",\n"
          + "       ORDINAL_POSITION AS attnum,\n"
          + "       IS_NULLABLE AS \"IS_NULLABLE\",\n"
          + "       NULL AS \"SCOPE_CATALOG\",\n"
          + "       NULL AS \"SCOPE_SCHEMA\",\n"
          + "       NULL AS \"SCOPE_TABLE\",\n"
          + "       NULL AS \"SOURCE_DATA_TYPE\",\n"
          + "       'NO' AS \"IS_AUTOINCREMENT\",\n"
          + "       CASE\n"
          + "           WHEN (IS_GENERATED = 'NEVER') THEN 'NO'\n"
          + "           ELSE 'YES'\n"
          + "        END AS \"IS_GENERATEDCOLUMN\"\n"
          + "FROM INFORMATION_SCHEMA.COLUMNS C\n";

  public static final String PG_JDBC_GET_INDEXES_PREFIX_1 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,  ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME,  CASE i.indisclustered  WHEN true THEN 1 ELSE CASE am.amname  WHEN 'hash' THEN 2 ELSE 3 END  END AS TYPE,  a.attnum AS ORDINAL_POSITION,  CASE WHEN i.indexprs IS NULL THEN a.attname  ELSE pg_catalog.pg_get_indexdef(ci.oid,a.attnum,false) END AS COLUMN_NAME,  NULL AS ASC_OR_DESC,  ci.reltuples AS CARDINALITY,  ci.relpages AS PAGES,  pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class ct, pg_catalog.pg_class ci,  pg_catalog.pg_attribute a, pg_catalog.pg_am am , pg_catalog.pg_index i";
  public static final String PG_JDBC_GET_INDEXES_REPLACEMENT =
      "SELECT IDX.TABLE_CATALOG AS \"TABLE_CAT\", IDX.TABLE_SCHEMA AS \"TABLE_SCHEM\", IDX.TABLE_NAME AS \"TABLE_NAME\",\n"
          + "       CASE WHEN IS_UNIQUE='YES' THEN FALSE ELSE TRUE END AS \"NON_UNIQUE\",\n"
          + "       IDX.TABLE_CATALOG AS \"INDEX_QUALIFIER\", IDX.INDEX_NAME AS \"INDEX_NAME\",\n"
          + "       2 AS \"TYPE\",\n"
          + "       ORDINAL_POSITION AS \"ORDINAL_POSITION\", COLUMN_NAME AS \"COLUMN_NAME\", SUBSTR(COLUMN_ORDERING, 1, 1) AS \"ASC_OR_DESC\",\n"
          + "       -1 AS \"CARDINALITY\", -- Not supported\n"
          + "       -1 AS \"PAGES\", -- Not supported\n"
          + "       NULL AS \"FILTER_CONDITION\"\n"
          + "FROM INFORMATION_SCHEMA.INDEXES IDX\n"
          + "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS COL\n"
          + "            ON  COALESCE(IDX.TABLE_CATALOG, '')=COALESCE(COL.TABLE_CATALOG, '')\n"
          + "                AND IDX.TABLE_SCHEMA=COL.TABLE_SCHEMA\n"
          + "                AND IDX.TABLE_NAME=COL.TABLE_NAME\n"
          + "                AND IDX.INDEX_NAME=COL.INDEX_NAME\n";

  public static final String PG_JDBC_GET_PRIMARY_KEY_PREFIX_1 =
      "SELECT        result.TABLE_CAT,        result.TABLE_SCHEM,        result.TABLE_NAME,        result.COLUMN_NAME,        result.KEY_SEQ,        result.PK_NAME FROM      (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)";
  public static final String PG_JDBC_GET_PRIMARY_KEY_PREFIX_2 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary,              information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)";
  public static final String PG_JDBC_GET_PRIMARY_KEY_REPLACEMENT =
      "SELECT IDX.TABLE_CATALOG AS \"TABLE_CAT\", IDX.TABLE_SCHEMA AS \"TABLE_SCHEM\", IDX.TABLE_NAME AS \"TABLE_NAME\",\n"
          + "       COLS.COLUMN_NAME AS \"COLUMN_NAME\", ORDINAL_POSITION AS \"KEY_SEQ\", IDX.INDEX_NAME AS \"PK_NAME\"\n"
          + "FROM INFORMATION_SCHEMA.INDEXES IDX\n"
          + "INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS COLS\n"
          + "            ON  COALESCE(IDX.TABLE_CATALOG, '')=COALESCE(COLS.TABLE_CATALOG, '')\n"
          + "                AND IDX.TABLE_SCHEMA=COLS.TABLE_SCHEMA\n"
          + "                AND IDX.TABLE_NAME=COLS.TABLE_NAME\n"
          + "                AND IDX.INDEX_NAME=COLS.INDEX_NAME\n";

  public static final String PG_JDBC_GET_TYPE_INFO_PREFIX_FULL =
      "SELECT t.typname,t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)  WHERE n.nspname  != 'pg_toast' AND  (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))";
  public static final String PG_JDBC_GET_TYPE_INFO_REPLACEMENT_FULL =
      " select 'bool' as typname, 16 as oid union all \n"
          + " select 'bytea' as typname, 17 as oid union all \n"
          + " select 'int8' as typname, 20 as oid union all \n"
          + " select 'float8' as typname, 701 as oid union all \n"
          + " select 'varchar' as typname, 1043 as oid union all \n"
          + " select 'date' as typname, 1082 as oid union all \n"
          + " select 'timestamptz' as typname, 1184 as oid union all \n"
          + " select 'numeric' as typname, 1700 as oid union all \n"
          + " select 'jsonb' as typname, 3802 as oid union all \n"
          + " select '_bool' as typname, 1000 as oid union all \n"
          + " select '_bytea' as typname, 1001 as oid union all \n"
          + " select '_int8' as typname, 1016 as oid union all \n"
          + " select '_float8' as typname, 1022 as oid union all \n"
          + " select '_varchar' as typname, 1015 as oid union all \n"
          + " select '_date' as typname, 1182 as oid union all \n"
          + " select '_timestamptz' as typname, 1185 as oid union all \n"
          + " select '_numeric' as typname, 1231 as oid union all \n"
          + " select '_jsonb' as typname, 3807 as oid";
  public static final String PG_JDBC_GET_TYPE_INFO_WITH_TYPTYPE_PREFIX =
      "SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  ORDER BY sp.r, pg_type.oid DESC";
  public static final String PG_JDBC_GET_TYPE_INFO_WITH_TYPTYPE_REPLACEMENT =
      "select true as is_array, 'b' as typtype, '_jsonb' as typname, 3807 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'jsonb' as typname, 3802 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'numeric' as typname, 1700 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_numeric' as typname, 1231 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_timestamptz' as typname, 1185 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'timestamptz' as typname, 1184 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_date' as typname, 1182 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'date' as typname, 1082 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'varchar' as typname, 1043 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_float8' as typname, 1022 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_int8' as typname, 1016 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_varchar' as typname, 1015 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_bytea' as typname, 1001 as oid union all\n"
          + " select true as is_array, 'b' as typtype, '_bool' as typname, 1000 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'float8' as typname, 701 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'int8' as typname, 20 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'bytea' as typname, 17 as oid union all\n"
          + " select false as is_array, 'b' as typtype, 'bool' as typname, 16 as oid";
  public static final String PG_JDBC_GET_TYPE_INFO_WITH_PARAMETER_PREFIX =
      "SELECT pg_type.oid, typname   FROM pg_catalog.pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE typname = $1  ORDER BY sp.r, pg_type.oid DESC LIMIT 1";
  public static final String PG_JDBC_GET_TYPE_INFO_WITH_PARAMETER_REPLACEMENT =
      "select 16 as oid, 'bool' as typname from (select 1) t where 'bool'=$1 union all \n"
          + " select 17 as oid, 'bytea' as typname from (select 1) t where 'bytea'=$1 union all \n"
          + " select 20 as oid, 'int8' as typname from (select 1) t where 'int8'=$1 union all \n"
          + " select 701 as oid, 'float8' as typname from (select 1) t where 'float8'=$1 union all \n"
          + " select 1043 as oid, 'varchar' as typname from (select 1) t where 'varchar'=$1 union all \n"
          + " select 1082 as oid, 'date' as typname from (select 1) t where 'date'=$1 union all \n"
          + " select 1184 as oid, 'timestamptz' as typname from (select 1) t where 'timestamptz'=$1 union all \n"
          + " select 1700 as oid, 'numeric' as typname from (select 1) t where 'numeric'=$1 union all \n"
          + " select 3802 as oid, 'jsonb' as typname from (select 1) t where 'jsonb'=$1 union all \n"
          + " select 1000 as oid, '_bool' as typname from (select 1) t where '_bool'=$1 union all \n"
          + " select 1001 as oid, '_bytea' as typname from (select 1) t where '_bytea'=$1 union all \n"
          + " select 1016 as oid, '_int8' as typname from (select 1) t where '_int8'=$1 union all \n"
          + " select 1022 as oid, '_float8' as typname from (select 1) t where '_float8'=$1 union all \n"
          + " select 1015 as oid, '_varchar' as typname from (select 1) t where '_varchar'=$1 union all \n"
          + " select 1182 as oid, '_date' as typname from (select 1) t where '_date'=$1 union all \n"
          + " select 1185 as oid, '_timestamptz' as typname from (select 1) t where '_timestamptz'=$1 union all \n"
          + " select 1231 as oid, '_numeric' as typname from (select 1) t where '_numeric'=$1 union all \n"
          + " select 3807 as oid, '_jsonb' as typname from (select 1) t where '_jsonb'=$1";
  public static final String PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PREFIX =
      "SELECT typinput='array_in'::regproc as is_array, typtype, typname   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  ORDER BY sp.r, pg_type.oid DESC";
  public static final String PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_REPLACEMENT =
      "select true as is_array, 'b' as typtype, '_jsonb' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'jsonb' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'numeric' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_numeric' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_timestamptz' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'timestamptz' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_date' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'date' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'varchar' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_float8' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_int8' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_varchar' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_bytea' as typname union all\n"
          + " select true as is_array, 'b' as typtype, '_bool' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'float8' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'int8' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'bytea' as typname union all\n"
          + " select false as is_array, 'b' as typtype, 'bool' as typname";
  public static final String PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PARAM_PREFIX =
      "SELECT typinput='array_in'::regproc, typtype   FROM pg_catalog.pg_type   LEFT   JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE typname = $1  ORDER BY sp.r, pg_type.oid DESC LIMIT 1";
  public static final String PG_JDBC_GET_TYPE_INFO_WITHOUT_OID_PARAM_REPLACEMENT =
      "select false as is_array, 'b' as typtype from (select 1) t where 'bool'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'bytea'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'int8'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'float8'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'varchar'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'date'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'timestamptz'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'numeric'=$1 union all \n"
          + " select false as is_array, 'b' as typtype from (select 1) t where 'jsonb'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_bool'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_bytea'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_int8'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_float8'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_varchar'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_date'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_timestamptz'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_numeric'=$1 union all \n"
          + " select true as is_array, 'b' as typtype from (select 1) t where '_jsonb'=$1";
  public static final String PG_JDBC_GET_TYPE_INFO_NOT_TOAST_PREFIX =
      "SELECT t.typname,t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)  WHERE n.nspname  != 'pg_toast'";
  public static final String PG_JDBC_GET_TYPE_INFO_NOT_TOAST_REPLACEMENT =
      "select 'bool' as typname, 16 as oid union all\n"
          + " select 'bytea' as typname, 17 as oid union all\n"
          + " select 'int8' as typname, 20 as oid union all\n"
          + " select 'float8' as typname, 701 as oid union all\n"
          + " select 'varchar' as typname, 1043 as oid union all\n"
          + " select 'date' as typname, 1082 as oid union all\n"
          + " select 'timestamptz' as typname, 1184 as oid union all\n"
          + " select 'numeric' as typname, 1700 as oid union all\n"
          + " select 'jsonb' as typname, 3802 as oid union all\n"
          + " select '_bool' as typname, 1000 as oid union all\n"
          + " select '_bytea' as typname, 1001 as oid union all\n"
          + " select '_int8' as typname, 1016 as oid union all\n"
          + " select '_float8' as typname, 1022 as oid union all\n"
          + " select '_varchar' as typname, 1015 as oid union all\n"
          + " select '_date' as typname, 1182 as oid union all\n"
          + " select '_timestamptz' as typname, 1185 as oid union all\n"
          + " select '_numeric' as typname, 1231 as oid union all\n"
          + " select '_jsonb' as typname, 3807 as oid";

  public static final String PG_JDBC_GET_TABLE_PRIVILEGES_PREFIX_1 =
      "SELECT n.nspname,c.relname,r.rolname,c.relacl  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_roles r  WHERE c.relnamespace = n.oid  AND c.relowner = r.oid  AND c.relkind IN ('r','p'";
  public static final String PG_JDBC_GET_TABLE_PRIVILEGES_PREFIX_2 =
      "SELECT n.nspname,c.relname,r.rolname,c.relacl  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_roles r  WHERE c.relnamespace = n.oid  AND c.relowner = r.oid  AND c.relkind = 'r'";
  public static final String PG_JDBC_GET_TABLE_PRIVILEGES_REPLACEMENT =
      "select '' as nspname, '' as relname, '' as rolname, '' as relacl from (select 1) t where false";
  public static final String PG_JDBC_GET_COLUMN_PRIVILEGES_PREFIX_1 =
      "SELECT n.nspname,c.relname,r.rolname,c.relacl,  a.attname  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c,  pg_catalog.pg_roles r, pg_catalog.pg_attribute a  WHERE c.relnamespace = n.oid  AND c.relowner = r.oid  AND c.oid = a.attrelid  AND c.relkind = 'r'  AND a.attnum > 0 AND NOT a.attisdropped";
  public static final String PG_JDBC_GET_COLUMN_PRIVILEGES_REPLACEMENT =
      "select '' as nspname, '' as relname, '' as rolname, '' as relacl, '' as attname from (select 1) t where false";

  public static final String PG_JDBC_GET_BEST_ROW_IDENTIFIER_PREFIX =
      "SELECT a.attname, a.atttypid, atttypmod FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary,              information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) WHERE true";
  public static final String PG_JDBC_GET_BEST_ROW_IDENTIFIER_REPLACEMENT =
      "select '' as attname, 0 as atttypid, -1 as atttypmod from (select 1) t where false";

  private static final String PG_JDBC_PK_QUERY_INNER_PREFIX_42_3 =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, "
          + "  ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, "
          + "  (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME, "
          + "  information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM "
          + "FROM pg_catalog.pg_class ct "
          + "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
          + "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
          + "  JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) "
          + "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) "
          + "WHERE true ";
  public static final String PG_JDBC_PK_QUERY_PREFIX_42_3 =
      "SELECT "
          + "       result.TABLE_CAT, "
          + "       result.TABLE_SCHEM, "
          + "       result.TABLE_NAME, "
          + "       result.COLUMN_NAME, "
          + "       result.KEY_SEQ, "
          + "       result.PK_NAME "
          + "FROM "
          + "     ("
          + PG_JDBC_PK_QUERY_INNER_PREFIX_42_3;
  public static final String PG_JDBC_PK_QUERY_PREFIX =
      "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, "
          + "  ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, "
          + "  (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME "
          + "FROM pg_catalog.pg_class ct "
          + "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
          + "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
          + "  JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, "
          + "             information_schema._pg_expandarray(i.indkey) AS keys "
          + "        FROM pg_catalog.pg_index i) i "
          + "    ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
          + "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) "
          + "WHERE true ";
  public static final String PG_JDBC_BEST_ROW_IDENTIFIER_PREFIX =
      "SELECT a.attname, a.atttypid, atttypmod "
          + "FROM pg_catalog.pg_class ct "
          + "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) "
          + "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) "
          + "  JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, "
          + "             information_schema._pg_expandarray(i.indkey) AS keys "
          + "        FROM pg_catalog.pg_index i) i "
          + "    ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) "
          + "WHERE true ";
  public static final String PG_JDBC_PK_QUERY_REPLACEMENT =
      "select NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,\n"
          + "  ct.relname AS TABLE_NAME, i.column_name AS COLUMN_NAME,\n"
          + "  key_seq AS KEY_SEQ, pk_name AS PK_NAME, a.attname, a_spanner.spanner_atttypid as atttypid, a.atttypmod\n"
          + "from (\n"
          + "\tselect idx.index_name as pk_name, idx.table_schema, idx.table_name, c.column_name, c.ordinal_position as key_seq, true as indisprimary\n"
          + "\tfrom information_schema.index_columns c\n"
          + "\tinner join information_schema.indexes idx on idx.table_catalog=c.table_catalog and idx.table_schema=c.table_schema and idx.table_name=c.table_name and idx.index_name=c.index_name and idx.index_type='PRIMARY_KEY'\n"
          + ") i\n"
          + "inner join pg_catalog.pg_namespace n on n.nspname=i.table_schema\n"
          + "inner join pg_catalog.pg_class ct on ct.relnamespace=n.oid and ct.relname=i.table_name\n"
          + "INNER JOIN pg_catalog.pg_attribute a on a.attrelid=ct.oid and a.attname=i.column_name\n"
          + "WHERE true ";

  public static final String PG_JDBC_EXPORTED_IMPORTED_KEYS_42_0_PREFIX =
      "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, "
          + "NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, "
          + "pos.n AS KEY_SEQ, "
          + "CASE con.confupdtype "
          + " WHEN 'c' THEN "
          + DatabaseMetaData.importedKeyCascade
          + " WHEN 'n' THEN "
          + DatabaseMetaData.importedKeySetNull
          + " WHEN 'd' THEN "
          + DatabaseMetaData.importedKeySetDefault
          + " WHEN 'r' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'a' THEN "
          + DatabaseMetaData.importedKeyNoAction
          + " ELSE NULL END AS UPDATE_RULE, "
          + "CASE con.confdeltype "
          + " WHEN 'c' THEN "
          + DatabaseMetaData.importedKeyCascade
          + " WHEN 'n' THEN "
          + DatabaseMetaData.importedKeySetNull
          + " WHEN 'd' THEN "
          + DatabaseMetaData.importedKeySetDefault
          + " WHEN 'r' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'a' THEN "
          + DatabaseMetaData.importedKeyNoAction
          + " ELSE NULL END AS DELETE_RULE, "
          + "con.conname AS FK_NAME, pkic.relname AS PK_NAME, "
          + "CASE "
          + " WHEN con.condeferrable AND con.condeferred THEN "
          + DatabaseMetaData.importedKeyInitiallyDeferred
          + " WHEN con.condeferrable THEN "
          + DatabaseMetaData.importedKeyInitiallyImmediate
          + " ELSE "
          + DatabaseMetaData.importedKeyNotDeferrable
          + " END AS DEFERRABILITY "
          + " FROM "
          + " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
          + " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
          + " pg_catalog.pg_constraint con, "
          + " pg_catalog.generate_series(1, 32) pos(n), "
          + " pg_catalog.pg_class pkic";
  public static final String PG_JDBC_EXPORTED_IMPORTED_KEYS_PREFIX =
      "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, "
          + "NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, "
          + "pos.n AS KEY_SEQ, "
          + "CASE con.confupdtype "
          + " WHEN 'c' THEN "
          + DatabaseMetaData.importedKeyCascade
          + " WHEN 'n' THEN "
          + DatabaseMetaData.importedKeySetNull
          + " WHEN 'd' THEN "
          + DatabaseMetaData.importedKeySetDefault
          + " WHEN 'r' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'p' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'a' THEN "
          + DatabaseMetaData.importedKeyNoAction
          + " ELSE NULL END AS UPDATE_RULE, "
          + "CASE con.confdeltype "
          + " WHEN 'c' THEN "
          + DatabaseMetaData.importedKeyCascade
          + " WHEN 'n' THEN "
          + DatabaseMetaData.importedKeySetNull
          + " WHEN 'd' THEN "
          + DatabaseMetaData.importedKeySetDefault
          + " WHEN 'r' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'p' THEN "
          + DatabaseMetaData.importedKeyRestrict
          + " WHEN 'a' THEN "
          + DatabaseMetaData.importedKeyNoAction
          + " ELSE NULL END AS DELETE_RULE, "
          + "con.conname AS FK_NAME, pkic.relname AS PK_NAME, "
          + "CASE "
          + " WHEN con.condeferrable AND con.condeferred THEN "
          + DatabaseMetaData.importedKeyInitiallyDeferred
          + " WHEN con.condeferrable THEN "
          + DatabaseMetaData.importedKeyInitiallyImmediate
          + " ELSE "
          + DatabaseMetaData.importedKeyNotDeferrable
          + " END AS DEFERRABILITY "
          + " FROM "
          + " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, "
          + " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, "
          + " pg_catalog.pg_constraint con, "
          + " pg_catalog.generate_series(1, 32) pos(n), "
          + " pg_catalog.pg_class pkic";
  public static final String PG_JDBC_EXPORTED_IMPORTED_KEYS_REPLACEMENT =
      "SELECT PARENT.TABLE_CATALOG AS PKTABLE_CAT, PARENT.TABLE_SCHEMA AS PKTABLE_SCHEM, PARENT.TABLE_NAME AS PKTABLE_NAME,\n"
          + "       PARENT.COLUMN_NAME AS PKCOLUMN_NAME, CHILD.TABLE_CATALOG AS FKTABLE_CAT, CHILD.TABLE_SCHEMA AS FKTABLE_SCHEM,\n"
          + "       CHILD.TABLE_NAME AS FKTABLE_NAME, CHILD.COLUMN_NAME AS FKCOLUMN_NAME,\n"
          + "       CHILD.ORDINAL_POSITION AS KEY_SEQ,\n"
          + "       1 AS UPDATE_RULE, -- 1 = importedKeyRestrict\n"
          + "       1 AS DELETE_RULE, -- 1 = importedKeyRestrict\n"
          + "       CONSTRAINTS.CONSTRAINT_NAME AS FK_NAME, CONSTRAINTS.UNIQUE_CONSTRAINT_NAME AS PK_NAME,\n"
          + "       7 AS DEFERRABILITY -- 7 = importedKeyNotDeferrable\n"
          + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS CONSTRAINTS\n"
          + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CHILD  ON COALESCE(CONSTRAINTS.CONSTRAINT_CATALOG, '')=COALESCE(CHILD.CONSTRAINT_CATALOG, '') AND CONSTRAINTS.CONSTRAINT_SCHEMA= CHILD.CONSTRAINT_SCHEMA AND CONSTRAINTS.CONSTRAINT_NAME= CHILD.CONSTRAINT_NAME\n"
          + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE PARENT ON COALESCE(CONSTRAINTS.UNIQUE_CONSTRAINT_CATALOG, '')=COALESCE(PARENT.CONSTRAINT_CATALOG, '') AND CONSTRAINTS.UNIQUE_CONSTRAINT_SCHEMA=PARENT.CONSTRAINT_SCHEMA AND CONSTRAINTS.UNIQUE_CONSTRAINT_NAME=PARENT.CONSTRAINT_NAME AND PARENT.ORDINAL_POSITION=CHILD.POSITION_IN_UNIQUE_CONSTRAINT\n"
          + "WHERE TRUE\n";

  public static final String PG_DESCRIPTION =
      "select * from (select 0 as objoid, 0 as classoid, 0 as objsubid, '' as description) pg_description where false";
  public static final String PG_SETTINGS =
      "select 'max_index_keys'::varchar as name, '32'::varchar as setting, null::varchar as unit, 'Preset Options'::varchar as category, 'Shows the maximum number of index keys.'::varchar as short_desc, ''::varchar as extra_desc, 'internal'::varchar as context,\n"
          + "\t'integer'::varchar as vartype, 'default'::varchar as source, '32'::varchar as min_val, '32'::varchar as max_val, null::varchar as enumvals, '32'::varchar as boot_val, '32'::varchar as reset_val,\n"
          + "\t''::varchar as sourcefile, 0::bigint as sourceline, false::bool as pending_restart";
  public static final String PG_AM =
      "select 1 as oid, 'spanner' as amname, 'spanner_default_handler' as amhandler, 't' as amtype";
  public static final String PG_TYPE =
      "select\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 16\n"
          + "        when 'pg_attrdef'\tthen 17\n"
          + "        when 'pg_attribute'\tthen 701\n"
          + "        when 'pg_class'\t\tthen 20\n"
          + "        when 'pg_collation'\tthen 1700\n"
          + "        when 'pg_database'\tthen 1184\n"
          + "        when 'pg_index'\t\tthen 1043\n"
          + "        else 0\n"
          + "        end as oid,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'bool'\n"
          + "        when 'pg_attrdef'\tthen 'bytea'\n"
          + "        when 'pg_attribute'\tthen 'float8'\n"
          + "        when 'pg_class'\t\tthen 'int8'\n"
          + "        when 'pg_collation'\tthen 'numeric'\n"
          + "        when 'pg_database'\tthen 'timestamptz'\n"
          + "        when 'pg_index'\t\tthen 'varchar'\n"
          + "        else ''\n"
          + "        end as typname,\n"
          + "    (select oid from pg_catalog.pg_namespace where nspname='pg_catalog') as typnamespace,\n"
          + "    null as typowner,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 1\n"
          + "        when 'pg_attrdef'\tthen -1\n"
          + "        when 'pg_attribute'\tthen 8\n"
          + "        when 'pg_class'\t\tthen 8\n"
          + "        when 'pg_collation'\tthen -1\n"
          + "        when 'pg_database'\tthen 8\n"
          + "        when 'pg_index'\t\tthen -1\n"
          + "        else -1\n"
          + "        end as typlen,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen true\n"
          + "        when 'pg_attrdef'\tthen false\n"
          + "        when 'pg_attribute'\tthen true\n"
          + "        when 'pg_class'\t\tthen true\n"
          + "        when 'pg_collation'\tthen false\n"
          + "        when 'pg_database'\tthen true\n"
          + "        when 'pg_index'\t\tthen false\n"
          + "        else false\n"
          + "        end as typbyval,\n"
          + "    'b' as typtype,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'B'\n"
          + "        when 'pg_attrdef'\tthen 'U'\n"
          + "        when 'pg_attribute'\tthen 'N'\n"
          + "        when 'pg_class'\t\tthen 'N'\n"
          + "        when 'pg_collation'\tthen 'N'\n"
          + "        when 'pg_database'\tthen 'D'\n"
          + "        when 'pg_index'\t\tthen 'S'\n"
          + "        else ''\n"
          + "        end as typcategory,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen true\n"
          + "        when 'pg_attrdef'\tthen false\n"
          + "        when 'pg_attribute'\tthen true\n"
          + "        when 'pg_class'\t\tthen false\n"
          + "        when 'pg_collation'\tthen false\n"
          + "        when 'pg_database'\tthen true\n"
          + "        when 'pg_index'\t\tthen false\n"
          + "        else false\n"
          + "        end as typispreferred,\n"
          + "    true as typisdefined, true as typisdefined, ',' as typdelim, 0 as typrelid, '-' as typsubscript, 0 as typelem,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 1000\n"
          + "        when 'pg_attrdef'\tthen 1001\n"
          + "        when 'pg_attribute'\tthen 1022\n"
          + "        when 'pg_class'\t\tthen 1016\n"
          + "        when 'pg_collation'\tthen 1231\n"
          + "        when 'pg_database'\tthen 1185\n"
          + "        when 'pg_index'\t\tthen 1015\n"
          + "        else 0\n"
          + "        end as typarray,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'boolin'\n"
          + "        when 'pg_attrdef'\tthen 'byteain'\n"
          + "        when 'pg_attribute'\tthen 'float8in'\n"
          + "        when 'pg_class'\t\tthen 'int8in'\n"
          + "        when 'pg_collation'\tthen 'numeric_in'\n"
          + "        when 'pg_database'\tthen 'timestamptz_in'\n"
          + "        when 'pg_index'\t\tthen 'varcharin'\n"
          + "        else ''\n"
          + "        end as typinput,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'boolout'\n"
          + "        when 'pg_attrdef'\tthen 'byteaout'\n"
          + "        when 'pg_attribute'\tthen 'float8out'\n"
          + "        when 'pg_class'\t\tthen 'int8out'\n"
          + "        when 'pg_collation'\tthen 'numeric_out'\n"
          + "        when 'pg_database'\tthen 'timestamptz_out'\n"
          + "        when 'pg_index'\t\tthen 'varcharout'\n"
          + "        else ''\n"
          + "        end as typoutput,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'boolrecv'\n"
          + "        when 'pg_attrdef'\tthen 'bytearecv'\n"
          + "        when 'pg_attribute'\tthen 'float8recv'\n"
          + "        when 'pg_class'\t\tthen 'int8recv'\n"
          + "        when 'pg_collation'\tthen 'numeric_recv'\n"
          + "        when 'pg_database'\tthen 'timestamptz_recv'\n"
          + "        when 'pg_index'\t\tthen 'varcharrecv'\n"
          + "        else ''\n"
          + "        end as typreceive,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'boolsend'\n"
          + "        when 'pg_attrdef'\tthen 'byteasend'\n"
          + "        when 'pg_attribute'\tthen 'float8send'\n"
          + "        when 'pg_class'\t\tthen 'int8send'\n"
          + "        when 'pg_collation'\tthen 'numeric_send'\n"
          + "        when 'pg_database'\tthen 'timestamptz_send'\n"
          + "        when 'pg_index'\t\tthen 'varcharsend'\n"
          + "        else ''\n"
          + "        end as typsend,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen '-'\n"
          + "        when 'pg_attrdef'\tthen '-'\n"
          + "        when 'pg_attribute'\tthen '-'\n"
          + "        when 'pg_class'\t\tthen '-'\n"
          + "        when 'pg_collation'\tthen 'numerictypmodin'\n"
          + "        when 'pg_database'\tthen 'timestamptztypmodin'\n"
          + "        when 'pg_index'\t\tthen 'varchartypmodin'\n"
          + "        else ''\n"
          + "        end as typmodin,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen '-'\n"
          + "        when 'pg_attrdef'\tthen '-'\n"
          + "        when 'pg_attribute'\tthen '-'\n"
          + "        when 'pg_class'\t\tthen '-'\n"
          + "        when 'pg_collation'\tthen 'numerictypmodout'\n"
          + "        when 'pg_database'\tthen 'timestamptztypmodout'\n"
          + "        when 'pg_index'\t\tthen 'varchartypmodout'\n"
          + "        else ''\n"
          + "        end as typmodout,\n"
          + "    '-' as typanalyze,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'c'\n"
          + "        when 'pg_attrdef'\tthen 'i'\n"
          + "        when 'pg_attribute'\tthen 'd'\n"
          + "        when 'pg_class'\t\tthen 'd'\n"
          + "        when 'pg_collation'\tthen 'i'\n"
          + "        when 'pg_database'\tthen 'd'\n"
          + "        when 'pg_index'\t\tthen 'i'\n"
          + "        else ''\n"
          + "        end as typalign,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'p'\n"
          + "        when 'pg_attrdef'\tthen 'x'\n"
          + "        when 'pg_attribute'\tthen 'p'\n"
          + "        when 'pg_class'\t\tthen 'p'\n"
          + "        when 'pg_collation'\tthen 'm'\n"
          + "        when 'pg_database'\tthen 'p'\n"
          + "        when 'pg_index'\t\tthen 'x'\n"
          + "        else ''\n"
          + "        end as typstorage,\n"
          + "    false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null::varchar as typdefaultbin, null::varchar as typdefault, null as typacl,\n"
          + "    case table_name\n"
          + "        when 'pg_am'\t\tthen 'bool'\n"
          + "        when 'pg_attrdef'\tthen 'bytea'\n"
          + "        when 'pg_attribute'\tthen 'double precision'\n"
          + "        when 'pg_class'\t\tthen 'bigint'\n"
          + "        when 'pg_collation'\tthen 'numeric'\n"
          + "        when 'pg_database'\tthen 'timestamp with time zone'\n"
          + "        when 'pg_index'\t\tthen 'character varying'\n"
          + "        else ''\n"
          + "        end as spanner_data_type\n"
          + "from information_schema.tables\n"
          + "where table_schema='pg_catalog'\n"
          + "and table_name in ('pg_am', 'pg_attrdef', 'pg_attribute', 'pg_class', 'pg_collation', 'pg_database', 'pg_index')\n"
          + "order by case table_name\n"
          + "  when 'pg_am'         then 'bool'\n"
          + "  when 'pg_attrdef'    then 'bytea'\n"
          + "  when 'pg_attribute'  then 'float8'\n"
          + "  when 'pg_class'      then 'int8'\n"
          + "  when 'pg_collation'  then 'numeric'\n"
          + "  when 'pg_database'   then 'timestamptz'\n"
          + "  when 'pg_index'      then 'varchar'\n"
          + "  else ''\n"
          + "end";
  public static final String PG_ATTR_TYPE =
      "select\n"
          + "\ta.attrelid, a.attname,\n"
          + "\tcase c.data_type\n"
          + "\t\twhen 'boolean'\t\t\t\t\tthen 16\n"
          + "\t\twhen 'bytea'\t\t\t\t\tthen 17\n"
          + "\t\twhen 'double precision'\t\t\tthen 701\n"
          + "\t\twhen 'bigint'\t\t\t\t\tthen 20\n"
          + "\t\twhen 'numeric'\t\t\t\t\tthen 1700\n"
          + "\t\twhen 'timestamp with time zone'\tthen 1184\n"
          + "\t\twhen 'character varying'\t\tthen 1043\n"
          + "\t\telse 0\n"
          + "\tend as spanner_atttypid\n"
          + "from pg_catalog.pg_attribute a\n"
          + "inner join pg_catalog.pg_class rel on a.attrelid=rel.oid\n"
          + "inner join pg_catalog.pg_namespace n on rel.relnamespace=n.oid\n"
          + "inner join information_schema.columns c on n.nspname=c.table_schema and rel.relname=c.table_name and a.attname=c.column_name\n";
}
