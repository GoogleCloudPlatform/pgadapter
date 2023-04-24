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

import com.google.api.core.InternalApi;
import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer;
import com.google.cloud.spanner.pgadapter.utils.QueryPartReplacer.ReplacementStatus;
import com.google.cloud.spanner.pgadapter.utils.RegexQueryPartReplacer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

@InternalApi
public class PgCatalog {
  private static final ImmutableMap<TableOrIndexName, TableOrIndexName> DEFAULT_TABLE_REPLACEMENTS =
      ImmutableMap.<TableOrIndexName, TableOrIndexName>builder()
          .put(
              new TableOrIndexName("pg_catalog", "pg_namespace"),
              new TableOrIndexName(null, "pg_namespace"))
          .put(
              new TableOrIndexName(null, "pg_namespace"),
              new TableOrIndexName(null, "pg_namespace"))
          .put(
              new TableOrIndexName("pg_collation", "pg_collation"),
              new TableOrIndexName(null, "pg_collation"))
          .put(
              new TableOrIndexName(null, "pg_collation"),
              new TableOrIndexName(null, "pg_collation"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_class"),
              new TableOrIndexName(null, "pg_class"))
          .put(new TableOrIndexName(null, "pg_class"), new TableOrIndexName(null, "pg_class"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_attribute"),
              new TableOrIndexName(null, "pg_attribute"))
          .put(
              new TableOrIndexName(null, "pg_attribute"),
              new TableOrIndexName(null, "pg_attribute"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_attrdef"),
              new TableOrIndexName(null, "pg_attrdef"))
          .put(new TableOrIndexName(null, "pg_attrdef"), new TableOrIndexName(null, "pg_attrdef"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_constraint"),
              new TableOrIndexName(null, "pg_constraint"))
          .put(
              new TableOrIndexName(null, "pg_constraint"),
              new TableOrIndexName(null, "pg_constraint"))
          .put(new TableOrIndexName("pg_index", "pg_index"), new TableOrIndexName(null, "pg_index"))
          .put(new TableOrIndexName(null, "pg_index"), new TableOrIndexName(null, "pg_index"))
          .put(new TableOrIndexName("pg_catalog", "pg_proc"), new TableOrIndexName(null, "pg_proc"))
          .put(new TableOrIndexName(null, "pg_proc"), new TableOrIndexName(null, "pg_proc"))
          .put(new TableOrIndexName("pg_catalog", "pg_enum"), new TableOrIndexName(null, "pg_enum"))
          .put(new TableOrIndexName(null, "pg_enum"), new TableOrIndexName(null, "pg_enum"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_range"),
              new TableOrIndexName(null, "pg_range"))
          .put(new TableOrIndexName(null, "pg_range"), new TableOrIndexName(null, "pg_range"))
          .put(new TableOrIndexName("pg_catalog", "pg_type"), new TableOrIndexName(null, "pg_type"))
          .put(new TableOrIndexName(null, "pg_type"), new TableOrIndexName(null, "pg_type"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_sequence"),
              new TableOrIndexName(null, "pg_sequence"))
          .put(new TableOrIndexName(null, "pg_sequence"), new TableOrIndexName(null, "pg_sequence"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_sequences"),
              new TableOrIndexName(null, "pg_sequences"))
          .put(
              new TableOrIndexName(null, "pg_sequences"),
              new TableOrIndexName(null, "pg_sequences"))
          .put(
              new TableOrIndexName("information_schema", "sequences"),
              new TableOrIndexName(null, "pg_information_schema_sequences"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_extension"),
              new TableOrIndexName(null, "pg_extension"))
          .put(
              new TableOrIndexName(null, "pg_extension"),
              new TableOrIndexName(null, "pg_extension"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_settings"),
              new TableOrIndexName(null, "pg_settings"))
          .put(new TableOrIndexName(null, "pg_settings"), new TableOrIndexName(null, "pg_settings"))
          .build();

  private static final ImmutableList<QueryPartReplacer> DEFAULT_FUNCTION_REPLACEMENTS =
      ImmutableList.of(
          RegexQueryPartReplacer.replace(
              Pattern.compile("pg_catalog.pg_table_is_visible\\s*\\(.+\\)"),
              Suppliers.ofInstance("true")),
          RegexQueryPartReplacer.replace(
              Pattern.compile("pg_table_is_visible\\s*\\(.+\\)"), Suppliers.ofInstance("true")),
          RegexQueryPartReplacer.replace(
              Pattern.compile("=\\s*ANY\\s*\\(current_schemas\\(\\s*true\\s*\\)\\)"),
              Suppliers.ofInstance(" IN ('pg_catalog', 'public')")),
          RegexQueryPartReplacer.replace(
              Pattern.compile("=\\s*ANY\\s*\\(current_schemas\\(\\s*false\\s*\\)\\)"),
              Suppliers.ofInstance(" IN ('pg_catalog', 'public')")));

  private final ImmutableSet<String> checkPrefixes;

  private final ImmutableMap<TableOrIndexName, TableOrIndexName> tableReplacements;
  private final ImmutableMap<TableOrIndexName, PgCatalogTable> pgCatalogTables;

  private final ImmutableList<QueryPartReplacer> functionReplacements;

  private static final Map<TableOrIndexName, PgCatalogTable> DEFAULT_PG_CATALOG_TABLES =
      ImmutableMap.<TableOrIndexName, PgCatalogTable>builder()
          .put(new TableOrIndexName(null, "pg_namespace"), new PgNamespace())
          .put(new TableOrIndexName(null, "pg_collation"), new PgCollation())
          .put(new TableOrIndexName(null, "pg_proc"), new PgProc())
          .put(new TableOrIndexName(null, "pg_enum"), new EmptyPgEnum())
          .put(new TableOrIndexName(null, "pg_range"), new PgRange())
          .put(new TableOrIndexName(null, "pg_sequence"), new PgSequence())
          .put(new TableOrIndexName(null, "pg_sequences"), new PgSequences())
          .put(
              new TableOrIndexName(null, "pg_information_schema_sequences"),
              new InformationSchemaSequences())
          .put(new TableOrIndexName(null, "pg_extension"), new PgExtension())
          .build();
  private final SessionState sessionState;

  public PgCatalog(@Nonnull SessionState sessionState, @Nonnull WellKnownClient wellKnownClient) {
    this.sessionState = Preconditions.checkNotNull(sessionState);
    this.checkPrefixes = wellKnownClient.getPgCatalogCheckPrefixes();
    ImmutableMap.Builder<TableOrIndexName, TableOrIndexName> builder =
        ImmutableMap.<TableOrIndexName, TableOrIndexName>builder()
            .putAll(DEFAULT_TABLE_REPLACEMENTS);
    wellKnownClient
        .getTableReplacements()
        .forEach((k, v) -> builder.put(TableOrIndexName.parse(k), TableOrIndexName.parse(v)));
    this.tableReplacements = builder.build();

    ImmutableMap.Builder<TableOrIndexName, PgCatalogTable> pgCatalogTablesBuilder =
        ImmutableMap.<TableOrIndexName, PgCatalogTable>builder()
            .putAll(DEFAULT_PG_CATALOG_TABLES)
            .put(new TableOrIndexName(null, "pg_class"), new PgClass())
            .put(new TableOrIndexName(null, "pg_attribute"), new PgAttribute())
            .put(new TableOrIndexName(null, "pg_attrdef"), new PgAttrdef())
            .put(new TableOrIndexName(null, "pg_constraint"), new PgConstraint())
            .put(new TableOrIndexName(null, "pg_index"), new PgIndex())
            .put(new TableOrIndexName(null, "pg_type"), new PgType())
            .put(new TableOrIndexName(null, "pg_settings"), new PgSettings());
    wellKnownClient
        .getPgCatalogTables()
        .forEach((k, v) -> pgCatalogTablesBuilder.put(TableOrIndexName.parse(k), v));
    this.pgCatalogTables = pgCatalogTablesBuilder.build();

    this.functionReplacements =
        ImmutableList.<QueryPartReplacer>builder()
            .addAll(getDefaultFunctionReplacements())
            .add(
                RegexQueryPartReplacer.replace(
                    Pattern.compile("version\\(\\)"),
                    () -> "'" + sessionState.getServerVersion() + "'"))
            .addAll(wellKnownClient.getQueryPartReplacements())
            .build();
  }

  @VisibleForTesting
  ImmutableList<QueryPartReplacer> getDefaultFunctionReplacements() {
    return DEFAULT_FUNCTION_REPLACEMENTS;
  }

  /** Replace supported pg_catalog tables with Common Table Expressions. */
  public Statement replacePgCatalogTables(Statement statement) {
    // Only replace tables if the statement contains at least one of the known prefixes.
    String sql = statement.getSql().toLowerCase(Locale.ENGLISH);
    if (checkPrefixes.stream().noneMatch(sql::contains)) {
      return statement;
    }

    Tuple<Set<TableOrIndexName>, Statement> replacedTablesStatement =
        new TableParser(statement).detectAndReplaceTables(tableReplacements);
    if (replacedTablesStatement.x().isEmpty()) {
      return replacedTablesStatement.y();
    }
    // Add Common Table Expressions for the pg_catalog tables that were detected and replaced in the
    // statement.
    Set<TableOrIndexName> alreadyAdded = new HashSet<>();
    ImmutableList.Builder<String> cteBuilder = ImmutableList.builder();
    for (TableOrIndexName table : replacedTablesStatement.x()) {
      addPgCatalogTable(table, getPgCatalogTable(table), cteBuilder, alreadyAdded);
    }

    return addCommonTableExpressions(replacedTablesStatement.y(), cteBuilder.build());
  }

  Tuple<String, ReplacementStatus> replaceKnownUnsupportedFunctions(Statement statement) {
    String sql = statement.getSql();
    for (QueryPartReplacer functionReplacement : functionReplacements) {
      Tuple<String, ReplacementStatus> result = functionReplacement.replace(sql);
      if (result.y() == ReplacementStatus.STOP) {
        return result;
      }
      sql = result.x();
    }
    return Tuple.of(sql, ReplacementStatus.CONTINUE);
  }

  Statement addCommonTableExpressions(Statement statement, ImmutableList<String> tableExpressions) {
    if (tableExpressions.isEmpty()) {
      return statement;
    }

    Tuple<String, ReplacementStatus> result = replaceKnownUnsupportedFunctions(statement);
    String sql = result.x();
    Statement.Builder builder;
    if (result.y() == ReplacementStatus.CONTINUE) {
      SimpleParser parser = new SimpleParser(sql);
      boolean hadCommonTableExpressions = parser.eatKeyword("with");
      String tableExpressionsSql = String.join(",\n", tableExpressions);
      builder =
          Statement.newBuilder("with ")
              .append(tableExpressionsSql)
              .append(hadCommonTableExpressions ? ",\n" : "\n");
      if (hadCommonTableExpressions) {
        // Include the entire original statement except the 'with' keyword.
        builder
            .append(parser.getSql().substring(0, parser.getPos() - 4))
            .append(parser.getSql().substring(parser.getPos()));
      } else {
        // Include the entire original statement (including any comments at the beginning).
        builder.append(parser.getSql());
      }
    } else {
      builder = Statement.newBuilder(sql);
    }
    Map<String, Value> parameters = statement.getParameters();
    for (Entry<String, Value> param : parameters.entrySet()) {
      builder.bind(param.getKey()).to(param.getValue());
    }
    statement = builder.build();
    return statement;
  }

  void addPgCatalogTable(
      TableOrIndexName tableName,
      PgCatalogTable pgCatalogTable,
      ImmutableList.Builder<String> cteBuilder,
      Set<TableOrIndexName> alreadyAdded) {
    if (pgCatalogTable == null) {
      return;
    }
    for (TableOrIndexName dependency : pgCatalogTable.getDependencies()) {
      addPgCatalogTable(dependency, getPgCatalogTable(dependency), cteBuilder, alreadyAdded);
    }
    if (alreadyAdded.add(tableName)) {
      cteBuilder.add(pgCatalogTable.getTableExpression());
    }
  }

  PgCatalogTable getPgCatalogTable(TableOrIndexName tableOrIndexName) {
    if (pgCatalogTables.containsKey(tableOrIndexName)) {
      return pgCatalogTables.get(tableOrIndexName);
    }
    return null;
  }

  @InternalApi
  public interface PgCatalogTable {
    String getTableExpression();

    default ImmutableSet<TableOrIndexName> getDependencies() {
      return ImmutableSet.of();
    }
  }

  @InternalApi
  public static class PgNamespace implements PgCatalogTable {
    public static final String PG_NAMESPACE_CTE =
        "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")";

    @Override
    public String getTableExpression() {
      return PG_NAMESPACE_CTE;
    }
  }

  @InternalApi
  public static class PgCollation implements PgCatalogTable {
    public static final String PG_COLLATION_CTE =
        "pg_collation as (\n"
            + "  select 100::bigint as oid, 'default' as collname, 11 as collnamespace,\n"
            + "         null::bigint as collowner, 'd'::varchar as collprovider, true::bool as collisdeterministic,\n"
            + "         -1::bigint as collencoding, null::varchar as collcollate, null::varchar as collctype,\n"
            + "         null::bigint as collversion\n"
            + ")";

    @Override
    public String getTableExpression() {
      return PG_COLLATION_CTE;
    }
  }

  // This is defined outside the PgType class, because Java 8 does not allow static initialization
  // inside inner classes.
  @InternalApi
  public static final String PG_TYPE_CTE_EMULATED =
      PgType.PG_TYPE_CTE.replace("0 as typrelid", "'0'::varchar as typrelid");

  @InternalApi
  public class PgType implements PgCatalogTable {
    private final ImmutableSet<TableOrIndexName> DEPENDENCIES =
        ImmutableSet.of(new TableOrIndexName(null, "pg_namespace"));
    private static final String GENERATION_SQL =
        "select 'select '\n"
            + "  || oid || ' as oid, '\n"
            + "  || '''' || typname || ''' as typname, '\n"
            + "  || '(select oid from pg_namespace where nspname=''pg_catalog'') as typnamespace, '\n"
            + "  || 'null as typowner, '\n"
            + "  || typlen || ' as typlen, '\n"
            + "  || typbyval::text || ' as typbyval, '\n"
            + "  || '''' || typtype || ''' as typtype, '\n"
            + "  || '''' || typcategory || ''' as typcategory, '\n"
            + "  || typispreferred::text || ' as typispreferred, '\n"
            + "  || (case typname\n"
            + "        when 'int2' then 'false'\n"
            + "        when 'int4' then 'false'\n"
            + "        when 'float4' then 'false'\n"
            + "        when 'timestamp' then 'false'\n"
            + "        else 'true'\n"
            + "      end) || ' as typisdefined, '\n"
            + "  || '''' || typdelim || ''' as typdelim, '\n"
            + "  || typrelid || ' as typrelid, '\n"
            + "  || typelem || ' as typelem, '\n"
            + "  || typarray || ' as typarray, '\n"
            + "  || '''' || typinput || ''' as typinput, '\n"
            + "  || '''' || typoutput || ''' as typoutput, '\n"
            + "  || '''' || typreceive || ''' as typreceive, '\n"
            + "  || '''' || typsend || ''' as typsend, '\n"
            + "  || '''' || typmodin || ''' as typmodin, '\n"
            + "  || '''' || typmodout || ''' as typmodout, '\n"
            + "  || '''' || typanalyze || ''' as typanalyze, '\n"
            + "  || '''' || typalign || ''' as typalign, '\n"
            + "  || '''' || typstorage || ''' as typstorage, '\n"
            + "  || typnotnull::text || ' as typnotnull, '\n"
            + "  || typbasetype || ' as typbasetype, '\n"
            + "  || typtypmod || ' as typtypmod, '\n"
            + "  || coalesce(typndims || ' as typndims, ', 'null as typndims, ')\n"
            + "  || coalesce(typcollation || ' as typcollation, ', 'null as typcollation, ')\n"
            + "  || coalesce('''' || typdefaultbin || ''' as typdefaultbin, ', 'null as typdefaultbin, ')\n"
            + "  || coalesce('''' || typdefault || ''' as typdefault, ', 'null as typdefault, ')\n"
            + "  || 'null as typacl '\n"
            + "  || 'union all'\n"
            + "from pg_type\n"
            + "where typname in ('bool', 'bytea', 'int2', 'int4', 'int8', 'float4', 'float8',\n"
            + "                  'numeric', 'varchar', 'text', 'jsonb', 'timestamp', 'timestamptz', 'date')\n"
            + ";\n";
    public static final String PG_TYPE_CTE =
        "pg_type as (\n"
            + "  select 16 as oid, 'bool' as typname, 11 as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'boolean' as spanner_type union all\n"
            + "  select 17 as oid, 'bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bytea' as spanner_type union all\n"
            + "  select 20 as oid, 'int8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bigint' as spanner_type union all\n"
            + "  select 21 as oid, 'int2' as typname, 11 as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 23 as oid, 'int4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 25 as oid, 'text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 700 as oid, 'float4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 701 as oid, 'float8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'double precision' as spanner_type union all\n"
            + "  select 1043 as oid, 'varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'character varying' as spanner_type union all\n"
            + "  select 1082 as oid, 'date' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'date' as spanner_type union all\n"
            + "  select 1114 as oid, 'timestamp' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1184 as oid, 'timestamptz' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'timestamp with time zone' as spanner_type union all\n"
            + "  select 1700 as oid, 'numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'numeric' as spanner_type union all\n"
            + "  select 3802 as oid, 'jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'jsonb' as spanner_type union all\n"
            + "  select 1000 as oid, '_bool' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 16 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'boolean[]' as spanner_type union all\n"
            + "  select 1001 as oid, '_bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 17 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bytea[]' as spanner_type union all\n"
            + "  select 1016 as oid, '_int8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 20 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'bigint[]' as spanner_type union all\n"
            + "  select 1005 as oid, '_int2' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 21 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1007 as oid, '_int4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 23 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1009 as oid, '_text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 25 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1021 as oid, '_float4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1022 as oid, '_float8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 701 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'double precision[]' as spanner_type union all\n"
            + "  select 1015 as oid, '_varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1043 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'character varying[]' as spanner_type union all\n"
            + "  select 1182 as oid, '_date' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1082 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'date[]' as spanner_type union all\n"
            + "  select 1115 as oid, '_timestamp' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 1114 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, null as spanner_type union all\n"
            + "  select 1185 as oid, '_timestamptz' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1184 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'timestamp with time zone[]' as spanner_type union all\n"
            + "  select 1231 as oid, '_numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'numeric[]' as spanner_type union all\n"
            + "  select 3807 as oid, '_jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 3802 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl, 'jsonb[]' as spanner_type\n"
            + ")";

    @Override
    public String getTableExpression() {
      return sessionState.isEmulatePgClassTables() ? PG_TYPE_CTE_EMULATED : PG_TYPE_CTE;
    }

    @Override
    public ImmutableSet<TableOrIndexName> getDependencies() {
      return DEPENDENCIES;
    }
  }

  private class PgSettings implements PgCatalogTable {

    @Override
    public String getTableExpression() {
      return sessionState.generatePGSettingsCte();
    }
  }

  @InternalApi
  public class PgClass implements PgCatalogTable {
    public static final String PG_CLASS_CTE =
        "pg_class as (\n"
            + "  select\n"
            + "  %s as oid,\n"
            + "  table_name as relname,\n"
            + "  case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "  0 as reltype,\n"
            + "  0 as reloftype,\n"
            + "  0 as relowner,\n"
            + "  1 as relam,\n"
            + "  0 as relfilenode,\n"
            + "  0 as reltablespace,\n"
            + "  0 as relpages,\n"
            + "  0.0::float8 as reltuples,\n"
            + "  0 as relallvisible,\n"
            + "  0 as reltoastrelid,\n"
            + "  false as relhasindex,\n"
            + "  false as relisshared,\n"
            + "  'p' as relpersistence,\n"
            + "  CASE table_type WHEN 'BASE TABLE' THEN 'r' WHEN 'VIEW' THEN 'v' ELSE '' END as relkind,\n"
            + "  count(*) as relnatts,\n"
            + "  0 as relchecks,\n"
            + "  false as relhasrules,\n"
            + "  false as relhastriggers,\n"
            + "  false as relhassubclass,\n"
            + "  false as relrowsecurity,\n"
            + "  false as relforcerowsecurity,\n"
            + "  true as relispopulated,\n"
            + "  'n' as relreplident,\n"
            + "  false as relispartition,\n"
            + "  0 as relrewrite,\n"
            + "  0 as relfrozenxid,\n"
            + "  0 as relminmxid,\n"
            + "  '{}'::bigint[] as relacl,\n"
            + "  '{}'::text[] as reloptions,\n"
            + "  0 as relpartbound\n"
            + "from information_schema.tables t\n"
            + "inner join information_schema.columns using (table_catalog, table_schema, table_name)\n"
            + "group by t.table_name, t.table_schema, t.table_type\n"
            + "union all\n"
            + "select\n"
            + "    %s as oid,\n"
            + "    i.index_name as relname,\n"
            + "    case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "    0 as reltype,\n"
            + "    0 as reloftype,\n"
            + "    0 as relowner,\n"
            + "    1 as relam,\n"
            + "    0 as relfilenode,\n"
            + "    0 as reltablespace,\n"
            + "    0 as relpages,\n"
            + "    0.0::float8 as reltuples,\n"
            + "    0 as relallvisible,\n"
            + "    0 as reltoastrelid,\n"
            + "    false as relhasindex,\n"
            + "    false as relisshared,\n"
            + "    'p' as relpersistence,\n"
            + "    'i' as relkind,\n"
            + "    count(*) as relnatts,\n"
            + "    0 as relchecks,\n"
            + "    false as relhasrules,\n"
            + "    false as relhastriggers,\n"
            + "    false as relhassubclass,\n"
            + "    false as relrowsecurity,\n"
            + "    false as relforcerowsecurity,\n"
            + "    true as relispopulated,\n"
            + "    'n' as relreplident,\n"
            + "    false as relispartition,\n"
            + "    0 as relrewrite,\n"
            + "    0 as relfrozenxid,\n"
            + "    0 as relminmxid,\n"
            + "    '{}'::bigint[] as relacl,\n"
            + "    '{}'::text[] as reloptions,\n"
            + "    0 as relpartbound\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name, index_name)\n"
            + "group by i.index_name, i.table_name, i.table_schema\n"
            + ")";

    @Override
    public String getTableExpression() {
      if (sessionState.isEmulatePgClassTables()) {
        return String.format(
            PG_CLASS_CTE,
            "'''\"' || t.table_schema || '\".\"' || t.table_name || '\"'''",
            "'''\"' || i.table_schema || '\".\"' || i.table_name || '\".\"' || i.index_name || '\"'''");
      }
      return String.format(PG_CLASS_CTE, "-1", "-1");
    }
  }

  private static class PgProc implements PgCatalogTable {
    private static final String PG_PROC_CTE =
        "pg_proc as (\n"
            + "select * from ("
            + "select 0::bigint as oid, ''::varchar as proname, 0::bigint as pronamespace, 0::bigint as proowner, "
            + "0::bigint as prolang, 0.0::float8 as procost, 0.0::float8 as prorows, 0::bigint as provariadic, "
            + "''::varchar as prosupport, ''::varchar as prokind, false::bool as prosecdef, false::bool as proleakproof, "
            + "false::bool as proisstrict, false::bool as proretset, ''::varchar as provolatile, ''::varchar as proparallel, "
            + "0::bigint as pronargs, 0::bigint as pronargdefaults, 0::bigint as prorettype, 0::bigint as proargtypes, "
            + "'{}'::bigint[] as proallargtypes, '{}'::varchar[] as proargmodes, '{}'::text[] as proargnames, "
            + "''::varchar as proargdefaults, '{}'::bigint[] as protrftypes, ''::text as prosrc, ''::text as probin, "
            + "''::varchar as prosqlbody, '{}'::text[] as proconfig, '{}'::bigint[] as proacl\n"
            + ") proc where false)";

    @Override
    public String getTableExpression() {
      return PG_PROC_CTE;
    }
  }

  @InternalApi
  public static class PgRange implements PgCatalogTable {
    public static final String PG_RANGE_CTE =
        "pg_range as (\n"
            + "select * from ("
            + "select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, "
            + "0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
            + ") range where false)";

    @Override
    public String getTableExpression() {
      return PG_RANGE_CTE;
    }
  }

  @InternalApi
  public class PgAttribute implements PgCatalogTable {
    public static final String EMPTY_PG_ATTRIBUTE_CTE =
        "pg_attribute as (\n"
            + "select * from ("
            + "select 0::bigint as attrelid, '' as attname, 0::bigint as atttypid, 0::bigint as attstattarget, "
            + "0::bigint as attlen, 0::bigint as attnum, 0::bigint as attndims, -1::bigint as attcacheoff, "
            + "0::bigint as atttypmod, true as attbyval, '' as attalign, '' as attstorage, '' as attcompression, "
            + "false as attnotnull, true as atthasdef, false as atthasmissing, '' as attidentity, '' as attgenerated, "
            + "false as attisdropped, true as attislocal, 0 as attinhcount, 0 as attcollation, '{}'::bigint[] as attacl, "
            + "'{}'::text[] as attoptions, '{}'::text[] as attfdwoptions, null as attmissingval\n"
            + ") a where false)";

    public static final String PG_ATTRIBUTE_CTE =
        "pg_attribute as (\n"
            + "select  '''\"' || table_schema || '\".\"' || table_name || '\"''' as attrelid,\n"
            + "        column_name as attname,\n"
            + "        case regexp_replace(c.spanner_type, '\\(.*\\)', '')\n"
            + "            when 'boolean' then 16\n"
            + "            when 'bytea' then 17\n"
            + "            when 'bigint' then 20\n"
            + "            when 'double precision' then 701\n"
            + "            when 'character varying' then 1043\n"
            + "            when 'date' then 1082\n"
            + "            when 'timestamp with time zone' then 1184\n"
            + "            when 'numeric' then 1700\n"
            + "            when 'jsonb' then 3802\n"
            + "            when 'boolean[]' then 1000\n"
            + "            when 'bytea[]' then 1001\n"
            + "            when 'bigint[]' then 1016\n"
            + "            when 'double precision[]' then 1022\n"
            + "            when 'character varying[]' then 1015\n"
            + "            when 'date[]' then 1182\n"
            + "            when 'timestamp with time zone[]' then 1185\n"
            + "            when 'numeric[]' then 1231\n"
            + "            when 'jsonb[]' then 3807\n"
            + "            else 0\n"
            + "        end as atttypid,\n"
            + "        0::bigint as attstattarget,\n"
            + "        character_maximum_length as attlen, c.ordinal_position as attnum,\n"
            + "        case data_type when 'ARRAY' then 1::bigint else 0::bigint end as attndims,\n"
            + "        -1::bigint as attcacheoff,\n"
            + "        coalesce(c.character_maximum_length, -1::bigint) as atttypmod, true as attbyval,\n"
            + "        'i' as attalign, 'p' as attstorage, ''::varchar as attcompression,\n"
            + "        c.is_nullable='NO' as attnotnull,\n"
            + "        (c.column_default is not null or c.generation_expression is not null) as atthasdef,\n"
            + "        false as atthasmissing,'' as attidentity,\n"
            + "        case c.generation_expression is not null when true then 's' else '' end as attgenerated,\n"
            + "        false as attisdropped, true as attislocal, 0 as attinhcount, null::bigint as attcollation, '{}'::bigint[] as attacl,\n"
            + "        '{}'::text[] as attoptions, '{}'::text[] as attfdwoptions, null as attmissingval,\n"
            + "        c.spanner_type\n"
            + "from information_schema.columns c\n"
            + "union all\n"
            + "select  '''\"' || i.table_schema || '\".\"' || i.table_name || '\".\"' || i.index_name || '\"''' as attrelid,\n"
            + "        i.column_name as attname,\n"
            + "        case regexp_replace(c.spanner_type, '\\(.*\\)', '')\n"
            + "            when 'boolean' then 16\n"
            + "            when 'bytea' then 17\n"
            + "            when 'bigint' then 20\n"
            + "            when 'double precision' then 701\n"
            + "            when 'character varying' then 1043\n"
            + "            when 'date' then 1082\n"
            + "            when 'timestamp with time zone' then 1184\n"
            + "            when 'numeric' then 1700\n"
            + "            when 'jsonb' then 3802\n"
            + "            when 'boolean[]' then 1000\n"
            + "            when 'bytea[]' then 1001\n"
            + "            when 'bigint[]' then 1016\n"
            + "            when 'double precision[]' then 1022\n"
            + "            when 'character varying[]' then 1015\n"
            + "            when 'date[]' then 1182\n"
            + "            when 'timestamp with time zone[]' then 1185\n"
            + "            when 'numeric[]' then 1231\n"
            + "            when 'jsonb[]' then 3807\n"
            + "            else 0\n"
            + "        end as atttypid,\n"
            + "        0::bigint as attstattarget,\n"
            + "        character_maximum_length as attlen, c.ordinal_position as attnum,\n"
            + "        case data_type when 'ARRAY' then 1::bigint else 0::bigint end as attndims,\n"
            + "        -1::bigint as attcacheoff,\n"
            + "        coalesce(c.character_maximum_length, -1::bigint) as atttypmod, true as attbyval,\n"
            + "        'i' as attalign, 'p' as attstorage, ''::varchar as attcompression,\n"
            + "        c.is_nullable='NO' as attnotnull,\n"
            + "        (c.column_default is not null or c.generation_expression is not null) as atthasdef,\n"
            + "        false as atthasmissing,'' as attidentity,\n"
            + "        case c.generation_expression is not null when true then 's' else '' end as attgenerated,\n"
            + "        false as attisdropped, true as attislocal, 0 as attinhcount, null::bigint as attcollation, '{}'::bigint[] as attacl,\n"
            + "        '{}'::text[] as attoptions, '{}'::text[] as attfdwoptions, null as attmissingval,\n"
            + "        c.spanner_type\n"
            + "from information_schema.index_columns i\n"
            + "inner join information_schema.columns c using (table_catalog, table_schema, table_name, column_name)"
            + ")";

    @Override
    public String getTableExpression() {
      return sessionState.isEmulatePgClassTables() ? PG_ATTRIBUTE_CTE : EMPTY_PG_ATTRIBUTE_CTE;
    }
  }

  @InternalApi
  public class PgAttrdef implements PgCatalogTable {
    public static final String EMPTY_PG_ATTRDEF_CTE =
        "pg_attrdef as (\n"
            + "select * from ("
            + "select 0::bigint as oid, 0::bigint as adrelid, 0::bigint as adnum, ''::varchar as adbin"
            + ") d where false)";

    public static final String PG_ATTRDEF_CTE =
        "pg_attrdef as (\n"
            + "select  '''\"' || table_schema || '\".\"' || table_name || '\".\"' || column_name || '\"''' as oid,\n"
            + "        '''\"' || table_schema || '\".\"' || table_name || '\"''' as adrelid,\n"
            + "        ordinal_position as adnum,\n"
            + "        coalesce(column_default, generation_expression) as adbin\n"
            + "from information_schema.columns c\n"
            + "where coalesce(column_default, generation_expression) is not null\n"
            + ")";

    @Override
    public String getTableExpression() {
      return sessionState.isEmulatePgClassTables() ? PG_ATTRDEF_CTE : EMPTY_PG_ATTRDEF_CTE;
    }
  }

  public class PgConstraint implements PgCatalogTable {
    public static final String PG_CONSTRAINT_CTE =
        "pg_constraint as (\n"
            + "select\n"
            + "    '''\"' || tc.constraint_schema || '\".\"' || tc.constraint_name || '\"''' as oid,\n"
            + "    tc.constraint_name as conname, 2200 as connamespace,\n"
            + "    case tc.constraint_type\n"
            + "        when 'PRIMARY KEY' then 'p'\n"
            + "        when 'CHECK' then 'c'\n"
            + "        when 'FOREIGN KEY' then 'f'\n"
            + "        else ''\n"
            + "    end as contype, false as condeferrable, false as condeferred, true as convalidated,\n"
            + "    '''\"' || tc.table_schema || '\".\"' || tc.table_name || '\"''' as conrelid,\n"
            + "    0::bigint as contypid, '0'::varchar as conindid, '0'::varchar as conparentid,\n"
            + "    '''\"' || uc.table_schema || '\".\"' || uc.table_name || '\"''' as confrelid,\n"
            + "    case rc.update_rule\n"
            + "        when 'CASCADE' then 'c'\n"
            + "        when 'NO ACTION' then 'a'\n"
            + "    end as confupdtype,\n"
            + "    case rc.delete_rule\n"
            + "        when 'CASCADE' then 'c'\n"
            + "        when 'NO ACTION' then 'a'\n"
            + "    end as confdeltype, 's' as confmatchtype, true as conislocal, 0 as coninhcount,\n"
            + "    true as connoinherit, cck.column_keys as conkey, fck.column_keys as confkey,\n"
            + "    null::bigint[] as conpfeqop, null::bigint[] as conppeqop, null::bigint[] as conffeqop,\n"
            + "    null::bigint[] as confdelsetcols, null::bigint[] as conexclop, check_clause as conbin\n"
            + "from information_schema.table_constraints tc\n"
            + "left outer join information_schema.referential_constraints rc using (constraint_catalog, constraint_schema, constraint_name)\n"
            + "left outer join information_schema.table_constraints uc on uc.constraint_catalog=rc.unique_constraint_catalog and uc.constraint_schema=rc.unique_constraint_schema and uc.constraint_name=rc.unique_constraint_name\n"
            + "left outer join (select c.constraint_catalog,\n"
            + "                        c.constraint_schema,\n"
            + "                        c.constraint_name,\n"
            + "                        array_agg(col.ordinal_position) as column_keys\n"
            + "                 from information_schema.table_constraints c\n"
            + "                          inner join information_schema.constraint_column_usage ccu\n"
            + "                                     using (constraint_catalog, constraint_schema, constraint_name)\n"
            + "                          inner join information_schema.columns col\n"
            + "                                     on ccu.table_catalog = col.table_catalog and\n"
            + "                                        ccu.table_schema = col.table_schema and\n"
            + "                                        ccu.table_name = col.table_name and\n"
            + "                                        ccu.column_name = col.column_name\n"
            + "                 where not c.constraint_type='FOREIGN KEY'\n"
            + "                 group by c.constraint_catalog, c.constraint_schema,\n"
            + "                          c.constraint_name\n"
            + "                 union all\n"
            + "                 select c.constraint_catalog,\n"
            + "                        c.constraint_schema,\n"
            + "                        c.constraint_name,\n"
            + "                        array_agg(col.ordinal_position) as column_keys\n"
            + "                 from information_schema.table_constraints c\n"
            + "                          inner join information_schema.key_column_usage kcu\n"
            + "                                     using (constraint_catalog, constraint_schema, constraint_name)\n"
            + "                          inner join information_schema.columns col\n"
            + "                                     on kcu.table_catalog = col.table_catalog and\n"
            + "                                        kcu.table_schema = col.table_schema and\n"
            + "                                        kcu.table_name = col.table_name and\n"
            + "                                        kcu.column_name = col.column_name\n"
            + "                 where c.constraint_type='FOREIGN KEY'\n"
            + "                 group by c.constraint_catalog, c.constraint_schema,\n"
            + "                          c.constraint_name) cck on tc.constraint_catalog=cck.constraint_catalog and tc.constraint_schema=cck.constraint_schema and tc.constraint_name=cck.constraint_name\n"
            + "left outer join (select c.constraint_catalog,\n"
            + "                        c.constraint_schema,\n"
            + "                        c.constraint_name,\n"
            + "                        array_agg(col.ordinal_position) as column_keys\n"
            + "                 from information_schema.referential_constraints c\n"
            + "                          inner join information_schema.constraint_column_usage ccu\n"
            + "                                     on  c.unique_constraint_catalog=ccu.constraint_catalog\n"
            + "                                         and c.unique_constraint_schema=ccu.constraint_schema\n"
            + "                                         and c.unique_constraint_name=ccu.constraint_name\n"
            + "                          inner join information_schema.columns col\n"
            + "                                     on ccu.table_catalog = col.table_catalog and\n"
            + "                                        ccu.table_schema = col.table_schema and\n"
            + "                                        ccu.table_name = col.table_name and\n"
            + "                                        ccu.column_name = col.column_name\n"
            + "                 group by c.constraint_catalog, c.constraint_schema,\n"
            + "                          c.constraint_name) fck on tc.constraint_catalog=fck.constraint_catalog and tc.constraint_schema=fck.constraint_schema and tc.constraint_name=fck.constraint_name\n"
            + "left outer join information_schema.check_constraints cc on cc.constraint_catalog=tc.constraint_catalog and cc.constraint_schema=tc.constraint_schema and cc.constraint_name=tc.constraint_name\n"
            + "where tc.constraint_schema='public' and not substr(tc.constraint_name, 1, length('CK_IS_NOT_NULL_')) = 'CK_IS_NOT_NULL_'\n"
            + ")";

    public static final String EMPTY_PG_CONSTRAINT_CTE =
        "pg_constraint as (\n"
            + "select * from ("
            + "select    0::bigint as oid, ''::varchar as conname, 0::bigint as connamespace, ''  as contype,\n"
            + "          false as condeferrable, false as condeferred, true as convalidated,\n"
            + "          0::bigint as conrelid, 0::bigint as contypid, 0::bigint as conindid, 0::bigint as conparentid,\n"
            + "          0::bigint as confrelid, ''::varchar as confupdtype, ''::varchar as confdeltype, '' as confmatchtype,\n"
            + "          true as conislocal, 0 as coninhcount, true as connoinherit, null::bigint[] as conkey,\n"
            + "          null::bigint[] as confkey, null::bigint[] as conpfeqop, null::bigint[] as conppeqop,\n"
            + "          null::bigint[] as conffeqop, null::bigint[] as confdelsetcols, null::bigint[] as conexclop,\n"
            + "          '' as conbin\n"
            + ") c where false)";

    @Override
    public String getTableExpression() {
      return sessionState.isEmulatePgClassTables() ? PG_CONSTRAINT_CTE : EMPTY_PG_CONSTRAINT_CTE;
    }
  }

  @InternalApi
  public class PgIndex implements PgCatalogTable {
    public static final String PG_INDEX_CTE =
        "pg_index as (\n"
            + "select '''\"' || i.table_schema || '\".\"' || i.table_name || '\".\"' || i.index_name || '\"''' as indexrelid,\n"
            + "       '''\"' || i.table_schema || '\".\"' || i.table_name || '\"''' as indrelid,\n"
            + "       count(1) as indnatts, sum(case ic.ordinal_position is null when true then 0 else 1 end) as indnkeyatts,\n"
            + "       i.is_unique='YES' as indisunique, i.is_unique='YES' and i.is_null_filtered='NO' as indnullsnotdistinct,\n"
            + "       i.index_type='PRIMARY_KEY' as indisprimary, false as indisexclusion, true as indimmediate,\n"
            + "       false as indisclustered, true as indisvalid, false as indcheckxmin, true as indisready,\n"
            + "       true as indislive, false as indisreplident, string_agg(c.ordinal_position::varchar, ' ') as indkey,\n"
            + "       ''::varchar as indcollation, ''::varchar as indclass, ''::varchar as indoption,"
            + "       null::varchar as indexprs, i.filter as indpred\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns ic using (table_catalog, table_schema, table_name, index_name)\n"
            + "inner join information_schema.columns c\n"
            + "           on ic.table_catalog=c.table_catalog and ic.table_schema=c.table_schema and ic.table_name=c.table_name and ic.column_name=c.column_name\n"
            + "group by i.table_schema, i.table_name, i.index_name, i.is_unique, i.is_null_filtered, i.index_type, i.filter\n"
            + ")";

    public static final String EMPTY_PG_INDEX_CTE =
        "pg_index as (\n"
            + "select * from (\n"
            + "select 0::bigint as indexrelid,\n"
            + "       0::bigint as indrelid,\n"
            + "       0::bigint as indnatts, 0::bigint as indnkeyatts,\n"
            + "       false as indisunique, false as indnullsnotdistinct,\n"
            + "       false as indisprimary, false as indisexclusion, true as indimmediate,\n"
            + "       false as indisclustered, true as indisvalid, false as indcheckxmin, true as indisready,\n"
            + "       true as indislive, false as indisreplident, null::bigint[] as indkey,\n"
            + "       null::bigint[] as indcollation, null::bigint[] as indclass, null::bigint[] as indoption,\n"
            + "       null::varchar as indexprs, null::varchar as indpred\n"
            + ") i where false)";

    @Override
    public String getTableExpression() {
      return sessionState.isEmulatePgClassTables() ? PG_INDEX_CTE : EMPTY_PG_INDEX_CTE;
    }
  }

  @InternalApi
  public static class EmptyPgEnum implements PgCatalogTable {
    public static final String PG_ENUM_CTE =
        "pg_enum as (\n"
            + "select * from ("
            + "select 0::bigint as oid, 0::bigint as enumtypid, 0.0::float8 as enumsortorder, ''::varchar as enumlabel\n"
            + ") e where false)";

    @Override
    public String getTableExpression() {
      return PG_ENUM_CTE;
    }
  }

  private static class PgSequences implements PgCatalogTable {
    private static final String PG_SEQUENCES_CTE =
        "pg_sequences as (\n"
            + "select * from ("
            + "select ''::varchar as schemaname, ''::varchar as sequencename, ''::varchar as sequenceowner, "
            + "0::bigint as data_type, 0::bigint as start_value, 0::bigint as min_value, 0::bigint as max_value, "
            + "0::bigint as increment_by, false::bool as cycle, 0::bigint as cache_size, 0::bigint as last_value\n"
            + ") seq where false)";

    @Override
    public String getTableExpression() {
      return PG_SEQUENCES_CTE;
    }
  }

  private static class PgSequence implements PgCatalogTable {
    private static final String PG_SEQUENCE_CTE =
        "pg_sequence as (\n"
            + "select * from ("
            + "select 0::bigint as seqrelid, 0::bigint as seqtypid, 0::bigint as seqstart, "
            + "0::bigint as seqincrement, 0::bigint as seqmax, 0::bigint as seqmin, 0::bigint as seqcache, "
            + "false::bool as seqcycle\n"
            + ") seq where false)";

    @Override
    public String getTableExpression() {
      return PG_SEQUENCE_CTE;
    }
  }

  private static class InformationSchemaSequences implements PgCatalogTable {
    // The name of this CTE is a little strange, but that is to make sure it does not accidentally
    // collide with any user-defined table or view.
    private static final String INFORMATION_SCHEMA_SEQUENCES_CTE =
        "pg_information_schema_sequences as (\n"
            + "select * from ("
            + "select ''::varchar as sequence_catalog, ''::varchar as sequence_schema, ''::varchar as sequence_name, "
            + "''::varchar as data_type, 0::bigint as numeric_precision, 0::bigint as numeric_precision_radix, "
            + "''::varchar as start_value, ''::varchar as minimum_value, ''::varchar as maximum_value, "
            + "''::varchar as increment, 'NO'::varchar as cycle_option\n"
            + ") seq where false)";

    @Override
    public String getTableExpression() {
      return INFORMATION_SCHEMA_SEQUENCES_CTE;
    }
  }

  @InternalApi
  public static class PgExtension implements PgCatalogTable {
    public static final String PG_EXTENSION_CTE =
        "pg_extension as (\n"
            + "select * from ("
            + "select 0::bigint as oid, ''::varchar as extname, 0::bigint as extowner, "
            + "0::bigint as extnamespace, false::bool as extrelocatable, ''::varchar as extversion, "
            + "'{}'::bigint[] as extconfig, '{}'::text[] as extcondition\n"
            + ") ext where false)";

    @Override
    public String getTableExpression() {
      return PG_EXTENSION_CTE;
    }
  }
}
