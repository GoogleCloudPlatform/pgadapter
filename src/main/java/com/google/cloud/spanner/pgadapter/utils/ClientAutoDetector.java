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
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.error.SQLState;
import com.google.cloud.spanner.pgadapter.session.PGSetting;
import com.google.cloud.spanner.pgadapter.statements.PgCatalog.PgCatalogTable;
import com.google.cloud.spanner.pgadapter.statements.local.AbortTransaction;
import com.google.cloud.spanner.pgadapter.statements.local.DjangoGetTableNamesStatement;
import com.google.cloud.spanner.pgadapter.statements.local.ListDatabasesStatement;
import com.google.cloud.spanner.pgadapter.statements.local.LocalStatement;
import com.google.cloud.spanner.pgadapter.statements.local.SelectCurrentCatalogStatement;
import com.google.cloud.spanner.pgadapter.statements.local.SelectCurrentDatabaseStatement;
import com.google.cloud.spanner.pgadapter.statements.local.SelectCurrentSchemaStatement;
import com.google.cloud.spanner.pgadapter.statements.local.SelectVersionStatement;
import com.google.cloud.spanner.pgadapter.statements.local.StartTransactionIsolationLevelRepeatableRead;
import com.google.cloud.spanner.pgadapter.wireoutput.NoticeResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.NoticeResponse.NoticeSeverity;
import com.google.cloud.spanner.pgadapter.wireprotocol.ParseMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.postgresql.core.Oid;

/**
 * Utility class that tries to automatically detect well-known clients and drivers that are
 * connecting to PGAdapter.
 */
@InternalApi
public class ClientAutoDetector {
  public static final ImmutableList<LocalStatement> EMPTY_LOCAL_STATEMENTS = ImmutableList.of();
  public static final ImmutableList<LocalStatement> DEFAULT_LOCAL_STATEMENTS =
      ImmutableList.of(
          SelectCurrentSchemaStatement.INSTANCE,
          SelectCurrentDatabaseStatement.INSTANCE,
          SelectCurrentCatalogStatement.INSTANCE,
          SelectVersionStatement.INSTANCE,
          DjangoGetTableNamesStatement.INSTANCE);
  private static final ImmutableSet<String> DEFAULT_CHECK_PG_CATALOG_PREFIXES =
      ImmutableSet.of("pg_", "information_schema.");
  public static final String PGBENCH_USAGE_HINT =
      "See https://github.com/GoogleCloudPlatform/pgadapter/blob/-/docs/pgbench.md for how to use pgbench with PGAdapter";

  public enum WellKnownClient {
    PSQL {

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // PSQL makes it easy for us, as it sends its own name in the application_name parameter.
        return parameters.containsKey("application_name")
            && parameters.get("application_name").equals("psql");
      }

      @Override
      public ImmutableList<LocalStatement> getLocalStatements(ConnectionHandler connectionHandler) {
        if (connectionHandler.getServer().getOptions().useDefaultLocalStatements()) {
          return ImmutableList.<LocalStatement>builder()
              .addAll(DEFAULT_LOCAL_STATEMENTS)
              .add(new ListDatabasesStatement(connectionHandler))
              .build();
        }
        return ImmutableList.of(new ListDatabasesStatement(connectionHandler));
      }
    },
    PG_FDW {
      final ImmutableList<QueryPartReplacer> functionReplacements =
          ImmutableList.of(
              RegexQueryPartReplacer.replace(
                  Pattern.compile("format_type\\s*\\(\\s*atttypid\\s*,\\s*atttypmod\\s*\\)"),
                  "spanner_type as format_type"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("pg_get_expr\\s*\\(\\s*adbin\\s*,\\s*adrelid\\s*\\)"),
                  "adbin as pg_get_expr"));

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // postgres_fdw by default sends its own name.
        return parameters.containsKey("application_name")
            && parameters
                .get("application_name")
                .toLowerCase(Locale.ENGLISH)
                .contains("postgres_fdw");
      }

      @Override
      public ImmutableList<LocalStatement> getLocalStatements(ConnectionHandler connectionHandler) {
        if (connectionHandler.getServer().getOptions().useDefaultLocalStatements()) {
          return ImmutableList.<LocalStatement>builder()
              .addAll(DEFAULT_LOCAL_STATEMENTS)
              .add(StartTransactionIsolationLevelRepeatableRead.INSTANCE)
              .add(AbortTransaction.INSTANCE)
              .build();
        }
        return ImmutableList.of(StartTransactionIsolationLevelRepeatableRead.INSTANCE);
      }

      @Override
      public ImmutableList<QueryPartReplacer> getQueryPartReplacements() {
        return functionReplacements;
      }
    },
    PGBENCH {
      final ImmutableList<String> errorHints = ImmutableList.of(PGBENCH_USAGE_HINT);
      volatile long lastHintTimestampMillis = 0L;

      @Override
      public void reset() {
        lastHintTimestampMillis = 0L;
      }

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // PGBENCH makes it easy for us, as it sends its own name in the application_name parameter.
        return parameters.containsKey("application_name")
            && parameters.get("application_name").equals("pgbench");
      }

      @Override
      public ImmutableList<NoticeResponse> createStartupNoticeResponses(
          ConnectionHandler connection) {
        synchronized (PGBENCH) {
          // Only send the hint at most once every 30 seconds, to prevent benchmark runs that open
          // multiple connections from showing the hint every time.
          if (Duration.ofMillis(System.currentTimeMillis() - lastHintTimestampMillis).getSeconds()
              > 30L) {
            lastHintTimestampMillis = System.currentTimeMillis();
            return ImmutableList.of(
                new NoticeResponse(
                    connection.getConnectionMetadata().getOutputStream(),
                    SQLState.Success,
                    NoticeSeverity.INFO,
                    "Detected connection from pgbench",
                    PGBENCH_USAGE_HINT + "\n"));
          }
        }
        return super.createStartupNoticeResponses(connection);
      }

      @Override
      public ImmutableList<String> getErrorHints(PGException exception) {
        return errorHints;
      }
    },
    RAILS {
      final ImmutableList<QueryPartReplacer> functionReplacements =
          ImmutableList.of(
              RegexQueryPartReplacer.replaceAndStop(
                  Pattern.compile(
                      "SELECT\\s+a\\.attname\\s+"
                          + "FROM\\s+\\(\\s+"
                          + "SELECT\\s+indrelid\\s*,\\s*indkey\\s*,\\s*generate_subscripts\\s*\\(\\s*indkey\\s*,\\s*1\\)\\s*idx\\s+"
                          + "FROM\\s+pg_index\\s+"
                          + "WHERE indrelid\\s*=\\s*'\"?(.+?)\"?'::regclass\\s+"
                          + "AND indisprimary\\s*"
                          + "\\)\\s*i\\s+"
                          + "JOIN\\s+pg_attribute\\s+a\\s+"
                          + "ON\\s+a\\.attrelid\\s*=\\s*i\\.indrelid\\s+"
                          + "AND\\s+a\\.attnum\\s*=\\s*i\\.indkey\\[i\\.idx]\\s*"
                          + "ORDER\\s+BY\\s+i\\.idx"),
                  "SELECT ic.column_name as attname\n"
                      + "FROM information_schema.index_columns ic\n"
                      + "INNER JOIN information_schema.indexes i using (table_catalog, table_schema, table_name, index_name)\n"
                      + "WHERE ic.table_schema='public' and ic.table_name='$1'\n"
                      + "AND i.index_type='PRIMARY_KEY'\n"
                      + "ORDER BY ordinal_position"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "format_type\\s*\\(\\s*a\\.atttypid\\s*,\\s*a\\.atttypmod\\s*\\)"),
                  "a.spanner_type as format_type"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("pg_get_expr\\s*\\(\\s*d\\.adbin\\s*,\\s*d\\.adrelid\\s*\\)"),
                  "d.adbin as pg_get_expr"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("col_description\\s*\\(\\s*.+\\s*,\\s*.+\\s*\\)"), "''::varchar"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "pg_catalog\\.obj_description\\s*\\(\\s*.+\\s*,\\s*'pg_class'\\s*\\)\\s*AS\\s+"),
                  "''::varchar AS "),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "pg_catalog\\.obj_description\\s*\\(\\s*.+\\s*,\\s*'pg_class'\\s*\\)"),
                  "''::varchar AS obj_description"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("pg_get_indexdef\\s*\\(.+\\)"),
                  "'CREATE INDEX ON USING btree ( )'::varchar AS pg_get_indexdef"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("pg_get_constraintdef\\s*\\(.+\\)\\s*AS\\s+"), "conbin AS "),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("'\"(.+?)\"'::regclass"), "'''\"public\".\"$1\"'''"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "string_agg\\(enum\\.enumlabel, ',' ORDER BY enum\\.enumsortorder\\)"),
                  "''::varchar"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("(\\s+.+?)\\.oid::regclass::text"),
                  " substr($1.oid, 12, length($1.oid) - 13)"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "t\\.typinput\\s*=\\s*'array_in\\(\\s*cstring\\s*,\\s*oid,\\s*integer\\)'::regprocedure"),
                  "t.typinput='array_in'"),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("SELECT\\s+distinct\\s+i\\.relname\\s*,"), "SELECT i.relname,"));

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        return parameters.containsKey("application_name")
            && (parameters.get("application_name").endsWith("rake")
                || parameters.get("application_name").endsWith(".rb")
                || parameters.get("application_name").contains("rails"));
      }

      @Override
      public ImmutableList<QueryPartReplacer> getQueryPartReplacements() {
        return functionReplacements;
      }

      @Override
      public ImmutableMap<String, String> getDefaultParameters(Map<String, String> parameters) {
        return ImmutableMap.of("spanner.emulate_pg_class_tables", "true");
      }

      @Override
      public ImmutableList<String> getErrorHints(PGException exception) {
        if (exception.getMessage() != null
            && exception
                .getMessage()
                .contains("DDL statements are only allowed outside explicit transactions")) {
          return ImmutableList.of(
              "Using Ruby ActiveRecord migrations requires that the option 'spanner.ddl_transaction_mode=AutocommitExplicitTransaction' has been set. "
                  + "Please add \"spanner.ddl_transaction_mode\": \"AutocommitExplicitTransaction\" to the \"variables\" section of your database.yml file.\n"
                  + "See https://github.com/GoogleCloudPlatform/pgadapter/blob/-/samples/ruby/activerecord/README.md for more information.");
        }
        if (exception.getMessage() != null
            && exception.getMessage().contains("SELECT pg_try_advisory_lock")) {
          return ImmutableList.of(
              "PGAdapter does not support advisory locks. Please 'add advisory_locks: false' to your database.yml file. "
                  + "See https://edgeguides.rubyonrails.org/configuring.html#configuring-a-postgresql-database for more information.");
        }
        return super.getErrorHints(exception);
      }
    },
    JDBC {
      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // JDBC always sends the following startup parameters as the first five parameters (and has
        // done so for more than 5 years):
        // paramList.add(new String[]{"user", user});
        // paramList.add(new String[]{"database", database});
        // paramList.add(new String[]{"client_encoding", "UTF8"});
        // paramList.add(new String[]{"DateStyle", "ISO"});
        // paramList.add(new String[]{"TimeZone", createPostgresTimeZone()});
        if (orderedParameterKeys.size() < 5) {
          return false;
        }
        if (!orderedParameterKeys.get(0).equals("user")) {
          return false;
        }
        if (!orderedParameterKeys.get(1).equals("database")) {
          return false;
        }
        if (!orderedParameterKeys.get(2).equals("client_encoding")) {
          return false;
        }
        if (!orderedParameterKeys.get(3).equals("DateStyle")) {
          return false;
        }
        if (!orderedParameterKeys.get(4).equals("TimeZone")) {
          return false;
        }
        if (!parameters.get("client_encoding").equals("UTF8")) {
          return false;
        }
        if (parameters.get("options") != null
            && parameters.get("options").contains("spanner.well_known_client")) {
          return false;
        }
        return parameters.get("DateStyle").equals("ISO, MDY")
            || parameters.get("DateStyle").equals("ISO");
      }

      @Override
      public ImmutableMap<String, String> getDefaultParameters(Map<String, String> parameters) {
        return ImmutableMap.of(
            "spanner.guess_types", String.format("%d,%d", Oid.TIMESTAMPTZ, Oid.DATE));
      }

      @Override
      public ImmutableList<QueryPartReplacer> getDdlReplacements() {
        // Replace known metadata tables for Liquibase.
        return ImmutableList.of(
            RegexQueryPartReplacer.replace(
                Pattern.compile(
                    "CREATE\\s+TABLE\\s+(?:.*\\.)?databasechangeloglock\\s*\\(\\s*"
                        + "ID\\s+INTEGER\\s+NOT\\s+NULL\\s*,\\s*"
                        + "LOCKED\\s+BOOLEAN\\s+NOT\\s+NULL\\s*,\\s*"
                        + "LOCKGRANTED\\s+TIMESTAMP\\s+WITHOUT\\s+TIME\\s+ZONE\\s*,\\s*"
                        + "LOCKEDBY\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "CONSTRAINT\\s*databasechangeloglock_pkey\\s*PRIMARY\\s+KEY\\s*\\(ID\\)\\s*\\s*"
                        + "\\)\\s*",
                    Pattern.CASE_INSENSITIVE),
                "CREATE TABLE databasechangeloglock (\n"
                    + "    ID INTEGER NOT NULL,\n"
                    + "    LOCKED BOOLEAN NOT NULL,\n"
                    + "    LOCKGRANTED TIMESTAMPTZ,\n"
                    + "    LOCKEDBY VARCHAR(255),\n"
                    + "    PRIMARY KEY (ID)\n"
                    + ")"),
            RegexQueryPartReplacer.replace(
                Pattern.compile(
                    "CREATE\\s+TABLE\\s+(?:.*\\.)?databasechangelog\\s*\\(\\s*"
                        + "ID\\s+VARCHAR\\s*\\(255\\)\\s*NOT\\s+NULL\\s*,\\s*"
                        + "AUTHOR\\s+VARCHAR\\s*\\(255\\)\\s*NOT\\s+NULL\\s*,\\s*"
                        + "FILENAME\\s+VARCHAR\\s*\\(255\\)\\s*NOT\\s+NULL\\s*,\\s*"
                        + "DATEEXECUTED\\s+TIMESTAMP(?:.*)?\\s+NOT\\s+NULL\\s*,\\s*"
                        + "ORDEREXECUTED\\s+INTEGER\\s+NOT\\s+NULL\\s*,\\s*"
                        + "EXECTYPE\\s+VARCHAR\\s*\\(10\\)\\s*NOT\\s+NULL\\s*,\\s*"
                        + "MD5SUM\\s+VARCHAR\\s*\\(35\\)\\s*,\\s*"
                        + "DESCRIPTION\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "COMMENTS\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "TAG\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "LIQUIBASE\\s+VARCHAR\\s*\\(20\\)\\s*,\\s*"
                        + "CONTEXTS\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "LABELS\\s+VARCHAR\\s*\\(255\\)\\s*,\\s*"
                        + "DEPLOYMENT_ID\\s+VARCHAR\\s*\\(10\\)\\s*"
                        + "\\)\\s*"),
                "CREATE TABLE databasechangelog (\n"
                    + "    ID VARCHAR(255) NOT NULL PRIMARY KEY,\n"
                    + "    AUTHOR VARCHAR(255) NOT NULL,\n"
                    + "    FILENAME VARCHAR(255) NOT NULL,\n"
                    + "    DATEEXECUTED TIMESTAMPTZ NOT NULL,\n"
                    + "    ORDEREXECUTED INTEGER NOT NULL,\n"
                    + "    EXECTYPE VARCHAR(10) NOT NULL,\n"
                    + "    MD5SUM VARCHAR(35),\n"
                    + "    DESCRIPTION VARCHAR(255),\n"
                    + "    COMMENTS VARCHAR(255),\n"
                    + "    TAG VARCHAR(255),\n"
                    + "    LIQUIBASE VARCHAR(20),\n"
                    + "    CONTEXTS VARCHAR(255),\n"
                    + "    LABELS VARCHAR(255),\n"
                    + "    DEPLOYMENT_ID VARCHAR(10)\n"
                    + ")"));
      }
    },
    PGX {
      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // pgx does not send enough unique parameters for it to be auto-detected.
        return false;
      }

      @Override
      boolean isClient(List<ParseMessage> skippedParseMessages, ParseMessage parseMessage) {
        // pgx uses a relatively unique naming scheme for prepared statements (and uses prepared
        // statements for everything by default).
        return parseMessage.getName() != null
            && (parseMessage.getName().startsWith("lrupsc_")
                || parseMessage.getName().startsWith("stmtcache_"));
      }
    },
    NPGSQL {
      final ImmutableList<QueryPartReplacer> functionReplacements =
          ImmutableList.of(
              RegexQueryPartReplacer.replace(
                  Pattern.compile("elemproc\\.oid = elemtyp\\.typreceive"),
                  Suppliers.ofInstance("false")),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("proc\\.oid = typ\\.typreceive"), Suppliers.ofInstance("false")),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("WHEN proc\\.proname='array_recv' THEN typ\\.typelem"),
                  Suppliers.ofInstance("WHEN substr(typ.typname, 1, 1)='_' THEN typ.typelem")),
              RegexQueryPartReplacer.replace(
                  Pattern.compile(
                      "WHEN proc\\.proname='array_recv' THEN 'a' ELSE typ\\.typtype END AS typtype"),
                  Suppliers.ofInstance(
                      "WHEN substr(typ.typname, 1, 1)='_' THEN 'a' ELSE typ.typtype END AS typtype")));

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // npgsql does not send enough unique parameters for it to be auto-detected.
        return false;
      }

      private final String prefix =
          "SELECT version();%s"
              + "%s"
              + "SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid%s";
      private final String unixPrefix = String.format(prefix, "\n", "\n", "\n");
      private final String windowsPrefix = String.format(prefix, "\r\n", "\r\n", "\r\n");

      @Override
      boolean isClient(List<ParseMessage> skippedParseMessages, List<Statement> statements) {
        // The npgsql client always starts with sending a query that contains multiple statements
        // and that starts with the following prefix.
        return statements.size() == 1
            && statements.get(0).getSql().contains("SELECT version();")
            && statements.get(0).getSql().contains("SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid");
      }

      @Override
      public ImmutableList<QueryPartReplacer> getQueryPartReplacements() {
        return functionReplacements;
      }
    },
    SQLALCHEMY2 {
      final ImmutableList<QueryPartReplacer> functionReplacements =
          ImmutableList.of(
              RegexQueryPartReplacer.replace(
                  Pattern.compile("oid::regtype::text AS regtype"),
                  Suppliers.ofInstance("'' as regtype")),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("WHERE t\\.oid = to_regtype\\(\\$1\\)"),
                  Suppliers.ofInstance("WHERE t.typname = \\$1")),
              RegexQueryPartReplacer.replace(
                  Pattern.compile("COLLATE \"C\""), Suppliers.ofInstance("")));

      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // SQLAlchemy 2.x does not send enough unique parameters for it to be auto-detected.
        return false;
      }

      @Override
      boolean isClient(List<ParseMessage> skippedParseMessages, List<Statement> statements) {
        // SQLAlchemy always starts with the following (relatively unique) combination of queries:
        // 1. 'BEGIN' using the extended query protocol.
        // 2. 'select pg_catalog.version()' using the simple query protocol.
        return skippedParseMessages.size() == 1
            && skippedParseMessages.get(0).getSql().equals("BEGIN")
            && statements.size() == 1
            && statements.get(0).getSql().equals("select pg_catalog.version()");
      }

      @Override
      public ImmutableList<QueryPartReplacer> getQueryPartReplacements() {
        return functionReplacements;
      }
    },
    UNSPECIFIED {
      @Override
      boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters) {
        // Use UNSPECIFIED as default to prevent null checks everywhere and to ease the use of any
        // defaults defined in this enum.
        return DEFAULT_UNSPECIFIED.get();
      }

      @Override
      boolean isClient(List<ParseMessage> skippedParseMessages, List<Statement> statements) {
        // Use UNSPECIFIED as default to prevent null checks everywhere and to ease the use of any
        // defaults defined in this enum.
        return DEFAULT_UNSPECIFIED.get();
      }

      @Override
      boolean isClient(List<ParseMessage> skippedParseMessages, ParseMessage parseMessage) {
        // Use UNSPECIFIED as default to prevent null checks everywhere and to ease the use of any
        // defaults defined in this enum.
        return DEFAULT_UNSPECIFIED.get();
      }

      @Override
      boolean isClient(PGSetting setting) {
        // Use UNSPECIFIED as default to prevent null checks everywhere and to ease the use of any
        // defaults defined in this enum.
        return DEFAULT_UNSPECIFIED.get();
      }
    };

    /** Indicates whether UNSPECIFIED should be used as default (instead of <code>null</code>). */
    @VisibleForTesting static final AtomicBoolean DEFAULT_UNSPECIFIED = new AtomicBoolean(true);

    abstract boolean isClient(List<String> orderedParameterKeys, Map<String, String> parameters);

    boolean isClient(PGSetting setting) {
      return false;
    }

    /** Resets any cached or temporary settings for the client. */
    @VisibleForTesting
    public void reset() {}

    boolean isClient(List<ParseMessage> skippedParseMessages, List<Statement> statements) {
      return false;
    }

    boolean isClient(List<ParseMessage> skippedParseMessages, ParseMessage parseMessage) {
      return false;
    }

    public ImmutableList<LocalStatement> getLocalStatements(ConnectionHandler connectionHandler) {
      if (connectionHandler.getServer().getOptions().useDefaultLocalStatements()) {
        return DEFAULT_LOCAL_STATEMENTS;
      }
      return EMPTY_LOCAL_STATEMENTS;
    }

    public ImmutableSet<String> getPgCatalogCheckPrefixes() {
      return DEFAULT_CHECK_PG_CATALOG_PREFIXES;
    }

    public ImmutableMap<String, String> getTableReplacements() {
      return ImmutableMap.of();
    }

    public ImmutableMap<String, PgCatalogTable> getPgCatalogTables() {
      return ImmutableMap.of();
    }

    public ImmutableList<QueryPartReplacer> getQueryPartReplacements() {
      return ImmutableList.of();
    }

    public ImmutableList<QueryPartReplacer> getDdlReplacements() {
      return ImmutableList.of();
    }

    /** Creates specific notice messages for a client after startup. */
    public ImmutableList<NoticeResponse> createStartupNoticeResponses(
        ConnectionHandler connection) {
      return ImmutableList.of();
    }

    /** Returns the client-specific hint(s) that should be included with the given exception. */
    public ImmutableList<String> getErrorHints(PGException exception) {
      return ImmutableList.of();
    }

    public ImmutableMap<String, String> getDefaultParameters(Map<String, String> parameters) {
      return ImmutableMap.of();
    }
  }

  /**
   * Returns the {@link WellKnownClient} that the detector thinks is connecting to PGAdapter based
   * purely on the list of parameters. It will return UNSPECIFIED if no specific client could be
   * determined.
   */
  public static @Nonnull WellKnownClient detectClient(
      List<String> orderParameterKeys, Map<String, String> parameters) {
    for (WellKnownClient client : WellKnownClient.values()) {
      if (client.isClient(orderParameterKeys, parameters)) {
        return client;
      }
    }
    // The following line should never be reached.
    throw new IllegalStateException("UNSPECIFIED.isClient() should have returned true");
  }

  /**
   * Returns the {@link WellKnownClient} that the detector thinks is connected to PGAdapter based on
   * the given list of SQL statements that have been executed.
   */
  public static @Nonnull WellKnownClient detectClient(
      List<ParseMessage> skippedParseMessages, List<Statement> statements) {
    for (WellKnownClient client : WellKnownClient.values()) {
      if (client.isClient(skippedParseMessages, statements)) {
        return client;
      }
    }
    // The following line should never be reached.
    throw new IllegalStateException("UNSPECIFIED.isClient() should have returned true");
  }

  /**
   * Returns the {@link WellKnownClient} that the detector thinks is connected to PGAdapter based on
   * the Parse message that has been received.
   */
  public static @Nonnull WellKnownClient detectClient(
      List<ParseMessage> skippedParseMessages, ParseMessage parseMessage) {
    for (WellKnownClient client : WellKnownClient.values()) {
      if (client.isClient(skippedParseMessages, parseMessage)) {
        return client;
      }
    }
    // The following line should never be reached.
    throw new IllegalStateException("UNSPECIFIED.isClient() should have returned true");
  }

  /** Detect the client based on a session state setting. */
  public static @Nonnull WellKnownClient detectClient(@Nullable PGSetting setting) {
    if (setting == null || setting.getSetting() == null) {
      return WellKnownClient.UNSPECIFIED;
    }
    for (WellKnownClient client : WellKnownClient.values()) {
      if (client.name().equalsIgnoreCase(setting.getSetting()) || client.isClient(setting)) {
        return client;
      }
    }
    // The following line should never be reached.
    throw new IllegalStateException("UNSPECIFIED.isClient() should have returned true");
  }
}
