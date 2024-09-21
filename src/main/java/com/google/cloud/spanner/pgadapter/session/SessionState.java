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

package com.google.cloud.spanner.pgadapter.session;

import static com.google.cloud.spanner.pgadapter.session.CopySettings.initCopySettings;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.DdlTransactionMode;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.cloud.spanner.pgadapter.session.PGSetting.Context;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** {@link SessionState} contains all session variables for a connection. */
@InternalApi
public class SessionState {
  /**
   * This set contains the settings that show up in the pg_settings CTE. Not all settings are
   * included in the CTE because Cloud Spanner has a limit of max 60 union all clauses in a
   * sub-select.
   */
  private static final ImmutableSet<String> SUPPORTED_PG_SETTINGS_KEYS =
      ImmutableSet.of(
          "application_name",
          "bytea_output",
          "DateStyle",
          "default_transaction_isolation",
          "default_transaction_read_only",
          "extra_float_digits",
          "max_connections",
          "max_index_keys",
          "port",
          "search_path",
          "server_version",
          "server_version_num",
          "TimeZone",
          "transaction_isolation",
          "transaction_read_only",
          "spanner.ddl_transaction_mode",
          "spanner.replace_pg_catalog_tables");

  static final Map<String, PGSetting> SERVER_SETTINGS = new HashMap<>();

  static {
    for (PGSetting setting : PGSetting.read()) {
      SERVER_SETTINGS.put(toKey(null, setting.getName()), setting);
    }
  }

  private final Map<String, PGSetting> settings;

  /** transactionSettings are the modified session settings during a transaction. */
  private Map<String, PGSetting> transactionSettings;
  /** localSettings are the modified local settings during a transaction. */
  private Map<String, PGSetting> localSettings;

  public SessionState(OptionsMetadata options) {
    this(ImmutableMap.of(), options);
  }

  @VisibleForTesting
  SessionState(Map<String, PGSetting> extraServerSettings, OptionsMetadata options) {
    Preconditions.checkNotNull(extraServerSettings);
    Preconditions.checkNotNull(options);
    this.settings = new HashMap<>(SERVER_SETTINGS.size() + extraServerSettings.size());
    for (Entry<String, PGSetting> entry : SERVER_SETTINGS.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
    for (Entry<String, PGSetting> entry : extraServerSettings.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
    this.settings.get("server_version").initSettingValue(options.getServerVersion());
    this.settings.get("server_version_num").initSettingValue(options.getServerVersionNum());
    initSettingValue(
        "spanner.ddl_transaction_mode",
        MoreObjects.firstNonNull(options.getDdlTransactionMode(), DdlTransactionMode.Batch).name());
    initSettingValue(
        "spanner.replace_pg_catalog_tables", Boolean.toString(options.replacePgCatalogTables()));

    initCopySettings(this.settings);
  }

  @VisibleForTesting
  Map<String, PGSetting> getSettings() {
    return this.settings;
  }

  void initSettingValue(String key, String value) {
    PGSetting setting = this.settings.get(key);
    if (setting != null) {
      setting.initSettingValue(value);
    }
  }

  /**
   * Generates a Common Table Expression that represents the pg_settings table. Note that the
   * generated query adds two additional CTEs that could in theory hide existing user tables. It is
   * however strongly recommended that user tables never start with 'pg_', as all system tables in
   * PostgreSQL start with 'pg_' and 'pg_catalog' is by design always included in the search_path
   * and is by default the first entry on the search_path. This means that user tables that start
   * with 'pg_' always risk being hidden by user tables, unless pg_catalog has been explicitly added
   * to the search_path after one or more user schemas.
   */
  public String generatePGSettingsCte() {
    return "pg_settings_inmem_ as (\n"
        + getAll().stream()
            .filter(setting -> SUPPORTED_PG_SETTINGS_KEYS.contains(setting.getCasePreservingKey()))
            .map(PGSetting::getSelectStatement)
            .collect(Collectors.joining("\nunion all\n"))
        + "\n),\n"
        + "pg_settings_names_ as (\n"
        + "select name from pg_settings_inmem_\n"
        + "union\n"
        + "select name from pg_catalog.pg_settings\n"
        + "),\n"
        + "pg_settings as (\n"
        + "select n.name, "
        + generatePgSettingsColumnExpressions()
        + "\n"
        + "from pg_settings_names_ n\n"
        + "left join pg_settings_inmem_ s1 using (name)\n"
        + "left join pg_catalog.pg_settings s2 using (name)\n"
        + "order by name\n"
        + ")\n";
  }

  /**
   * Generates a string of `coalesce(s1.col, s2.col) as col` for all column names (except `name`) in
   * pg_settings.
   */
  private static String generatePgSettingsColumnExpressions() {
    return PGSetting.getColumnNames().stream()
        .skip(1)
        .map(column -> "coalesce(s1." + column + ", s2." + column + ") as " + column)
        .collect(Collectors.joining(","));
  }

  private static String toKey(String extension, String name) {
    if (extension == null && "timezone".equalsIgnoreCase(name)) {
      // TimeZone is the only special setting that uses CamelCase.
      return "TimeZone";
    }
    return extension == null
        ? name.toLowerCase(Locale.ROOT)
        : extension.toLowerCase(Locale.ROOT) + "." + name.toLowerCase(Locale.ROOT);
  }

  /** Sets the value of the specified setting at connection startup. */
  public void setConnectionStartupValue(String extension, String name, String value) {
    String key = toKey(extension, name);
    PGSetting setting = this.settings.get(key);
    if (setting == null && extension == null) {
      // Ignore unknown settings.
      return;
    }
    if (setting == null) {
      setting = new PGSetting(extension, name);
      this.settings.put(key, setting);
    }
    try {
      setting.initConnectionValue(value);
      // Also update server_version_num if the server_version is set in the connection startup
      // message. This is something that is not supported by PostgreSQL, but for PGAdapter the
      // minimum context needed for setting the server_version is BACKEND.
      if (key.equals(toKey(null, "server_version"))) {
        setting = this.settings.get(toKey(null, "server_version_num"));
        setting.setSetting(Context.INTERNAL, OptionsMetadata.toServerVersionNum(value));
      }
    } catch (Exception ignore) {
      // ignore errors in startup values to prevent unknown or invalid settings from stopping a
      // connection from being made.
    }
  }

  /**
   * Sets the value of the specified setting. The new value will be persisted if the current
   * transaction is committed. The value will be lost if the transaction is rolled back.
   */
  public void set(String extension, String name, String setting) {
    if (transactionSettings == null) {
      transactionSettings = new HashMap<>();
    }
    internalSet(extension, name, setting, transactionSettings);
    // Remove the setting from the local settings if it's there, as the new transaction setting is
    // the one that should be used.
    if (localSettings != null) {
      localSettings.remove(toKey(extension, name));
    }
  }

  /**
   * Sets the value of the specified setting for the current transaction. This value is lost when
   * the transaction is committed or rolled back.
   */
  public void setLocal(String extension, String name, String setting) {
    if (localSettings == null) {
      localSettings = new HashMap<>();
    }
    // Note that setting a local setting does not remove it from the transaction settings. This
    // means that a commit will persist the setting in transactionSettings.
    internalSet(extension, name, setting, localSettings);
  }

  private void internalSet(
      String extension, String name, String setting, Map<String, PGSetting> currentSettings) {
    clearCachedValues();
    String key = toKey(extension, name);
    PGSetting newSetting = currentSettings.get(key);
    if (newSetting == null) {
      PGSetting existingSetting = settings.get(key);
      if (existingSetting == null) {
        if (extension == null) {
          throw unknownParamError(key);
        }
        newSetting = new PGSetting(extension, name);
      } else {
        newSetting = existingSetting.copy();
      }
    }
    if (setting == null) {
      setting = newSetting.getResetVal();
    }
    // Consider all users as SUPERUSER.
    newSetting.setSetting(Context.SUPERUSER, setting);
    currentSettings.put(key, newSetting);
  }

  private void clearCachedValues() {
    cachedZoneId = null;
  }

  /** Returns the current value of the specified setting. */
  public PGSetting get(String extension, String name) {
    return internalGet(toKey(extension, name), true);
  }

  /** Returns the current value of the specified setting or null if undefined. */
  @Nullable
  public PGSetting tryGet(String extension, String name) {
    return internalGet(toKey(extension, name), false);
  }

  private PGSetting internalGet(String key, boolean throwForUnknownParam) {
    if (localSettings != null && localSettings.containsKey(key)) {
      return localSettings.get(key);
    }
    if (transactionSettings != null && transactionSettings.containsKey(key)) {
      return transactionSettings.get(key);
    }
    if (settings.containsKey(key)) {
      return settings.get(key);
    }
    if (throwForUnknownParam) {
      throw unknownParamError(key);
    }
    return null;
  }

  String getStringSetting(String extension, String name, String defaultValue) {
    PGSetting setting = internalGet(toKey(extension, name), false);
    return setting != null ? setting.getSetting() : defaultValue;
  }

  boolean getBoolSetting(String extension, String name, boolean defaultValue) {
    PGSetting setting = internalGet(toKey(extension, name), false);
    if (setting != null) {
      try {
        return BooleanParser.toBoolean(setting.getSetting());
      } catch (Exception ignore) {
      }
    }
    return defaultValue;
  }

  int getIntegerSetting(String extension, String name, int defaultValue) {
    PGSetting setting = internalGet(toKey(extension, name), false);
    if (setting != null) {
      try {
        return Integer.parseInt(setting.getSetting());
      } catch (Exception ignore) {
      }
    }
    return defaultValue;
  }

  float getFloatSetting(String extension, String name, float defaultValue) {
    PGSetting setting = internalGet(toKey(extension, name), false);
    if (setting != null) {
      try {
        return Float.parseFloat(setting.getSetting());
      } catch (Exception ignore) {
      }
    }
    return defaultValue;
  }

  /** Returns all settings and their current values. */
  public List<PGSetting> getAll() {
    List<PGSetting> result =
        new ArrayList<>(
            (localSettings == null ? 0 : localSettings.size())
                + (transactionSettings == null ? 0 : transactionSettings.size())
                + settings.size());
    Set<String> keys =
        Sets.union(
            settings.keySet(),
            Sets.union(
                localSettings == null ? Collections.emptySet() : localSettings.keySet(),
                transactionSettings == null
                    ? Collections.emptySet()
                    : transactionSettings.keySet()));
    for (String key : keys) {
      result.add(internalGet(key, true));
    }
    result.sort(Comparator.comparing(PGSetting::getCasePreservingKey));
    return result;
  }

  /** Resets all values to their 'reset' value. */
  public void resetAll() {
    for (PGSetting setting : getAll()) {
      if (setting.isSettable(Context.SUPERUSER)
          && !Objects.equals(setting.getSetting(), setting.getResetVal())) {
        set(setting.getExtension(), setting.getName(), setting.getResetVal());
      }
    }
  }

  static SpannerException unknownParamError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("unrecognized configuration parameter \"%s\"", key));
  }

  /**
   * Commits the current transaction and persists any changes to the settings (except local
   * changes).
   */
  public void commit() {
    if (transactionSettings != null) {
      for (PGSetting setting : transactionSettings.values()) {
        settings.put(toKey(setting.getExtension(), setting.getName()), setting);
      }
    }
    this.localSettings = null;
    this.transactionSettings = null;
  }

  /** Rolls back the current transaction and abandons any pending changes to the settings. */
  public void rollback() {
    this.localSettings = null;
    this.transactionSettings = null;
  }

  /** Returns the PostgreSQL version. */
  public String getServerVersion() {
    return getStringSetting(null, "server_version", OptionsMetadata.DEFAULT_SERVER_VERSION);
  }

  /** Returns the PostgreSQL version number. */
  public String getServerVersionNum() {
    return getStringSetting(
        null,
        "server_version_num",
        OptionsMetadata.toServerVersionNum(OptionsMetadata.DEFAULT_SERVER_VERSION));
  }

  /**
   * Returns whether COPY TO STDOUT operations should try to use PartitionQuery.
   *
   * <p>COPY TO STDOUT tries to use PartitionQuery by default and then executes the partitions in
   * parallel. This assumes that the COPY result is large and that it will benefit from the
   * additional parallelism. For smaller result sizes, the additional roundtrip for PartitionQuery
   * adds latency, and it is better to execute the query directly. This is also the case for queries
   * that are known to be non-partitionable.
   */
  public boolean isCopyPartitionQuery() {
    return getBoolSetting("spanner", "copy_partition_query", true);
  }

  /**
   * Returns whether transaction statements should be ignored and all statements should be executed
   * in autocommit mode.
   */
  public boolean isForceAutocommit() {
    return getBoolSetting("spanner", "force_autocommit", false);
  }

  /**
   * Returns whether DROP ... CASCADE statements are supported. This also includes implicit
   * cascading operations, such as dropping indexes of tables that are being dropped.
   */
  public boolean isSupportDropCascade() {
    return getBoolSetting("spanner", "support_drop_cascade", false);
  }

  /**
   * Returns whether statements with an OFFSET clause that uses a parameter should be automatically
   * appended with a LIMIT clause. The LIMIT clause will use the literal Long.MAX_VALUE for unbound
   * statements, and Long.MAX_VALUE - offset for bound statements.
   *
   * <p>This method will be removed in the future.
   */
  // TODO: Remove when Cloud Spanner supports parametrized OFFSET clauses without a LIMIT clause.
  @InternalApi
  public boolean isAutoAddLimitClause() {
    return getBoolSetting("spanner", "auto_add_limit_clause", false);
  }

  /**
   * Returns whether `FOR UPDATE` clauses will automatically be replaced with a
   * LOCK_SCANNED_RANGES=exclusive hint. These statements are sometimes generated by ORMs, like for
   * example Hibernate.
   *
   * <p>This method will be removed in the future.
   */
  // TODO: Remove when Cloud Spanner supports SELECT ... FOR UPDATE.
  public boolean isReplaceForUpdateClause() {
    return getBoolSetting("spanner", "replace_for_update", true);
  }

  /** Returns the current setting for replacing pg_catalog tables with common table expressions. */
  public boolean isReplacePgCatalogTables() {
    PGSetting setting = internalGet(toKey("spanner", "replace_pg_catalog_tables"), false);
    if (setting == null) {
      return true;
    }
    return tryGetFirstNonNull(
        true,
        () -> BooleanParser.toBoolean(setting.getSetting()),
        () -> BooleanParser.toBoolean(setting.getResetVal()),
        () -> BooleanParser.toBoolean(setting.getBootVal()));
  }

  /**
   * Returns the current setting for replacing pg_class tables with common table expressions that
   * use the object name as OID.
   */
  public boolean isEmulatePgClassTables() {
    PGSetting setting = internalGet(toKey("spanner", "emulate_pg_class_tables"), false);
    if (setting == null) {
      return false;
    }
    return tryGetFirstNonNull(
        true,
        () -> BooleanParser.toBoolean(setting.getSetting()),
        () -> BooleanParser.toBoolean(setting.getResetVal()),
        () -> BooleanParser.toBoolean(setting.getBootVal()));
  }

  /** Returns the {@link DdlTransactionMode} that is used for this connection at this time. */
  public DdlTransactionMode getDdlTransactionMode() {
    PGSetting setting = internalGet(toKey("spanner", "ddl_transaction_mode"), false);
    if (setting == null) {
      return DdlTransactionMode.Batch;
    }
    return tryGetFirstNonNull(
        DdlTransactionMode.Batch,
        () -> DdlTransactionMode.valueOf(setting.getSetting()),
        () -> DdlTransactionMode.valueOf(setting.getResetVal()),
        () -> DdlTransactionMode.valueOf(setting.getBootVal()));
  }

  /** Returns the threshold for when a query should be considered slow and should be logged. */
  public Duration getLogSlowStatementThreshold() {
    PGSetting setting = internalGet(toKey("spanner", "log_slow_statement_threshold"), false);
    if (setting == null) {
      return Duration.ofSeconds(120L);
    }
    return tryGetFirstNonNull(
        Duration.ofSeconds(120L),
        () -> Duration.parse(setting.getSetting()),
        () -> Duration.parse(setting.getResetVal()),
        () -> Duration.parse(setting.getBootVal()));
  }

  /**
   * Returns the buffer size to use for converting bytea values from the Spanner Base64 format to
   * the PostgreSQL wire-format. Zero means no buffer and instead convert it directly in all cases.
   */
  public int getBinaryConversionBufferSize() {
    return getIntegerSetting("spanner", "binary_conversion_buffer_size", 0);
  }

  /**
   * Returns the buffer size to use for converting string values to the PostgreSQL wire-format. Zero
   * means no buffer and instead convert it directly in all cases.
   */
  public int getStringConversionBufferSize() {
    return getIntegerSetting("spanner", "string_conversion_buffer_size", 0);
  }

  /**
   * Returns the update count that PGAdapter should return for DML statements that are executed
   * during a DML batch. The default is 0.
   *
   * <p>Example:
   *
   * <pre>{@code
   * begin;
   * set local spanner.dml_batch_update_count=1;
   * start batch dml;
   * insert into my_table (id, value) values (1, 'one'); -- This returns update count 1
   * insert into my_table (id, value) values (2, 'two'); -- This returns update count 1
   * run batch; -- This actually executes the DML statements.
   * commit;
   * }</pre>
   */
  public int getDmlBatchUpdateCount() {
    return getIntegerSetting("spanner", "dml_batch_update_count", 0);
  }

  private ZoneId cachedZoneId;

  /** Returns the {@link ZoneId} of the current timezone for this session. */
  public ZoneId getTimezone() {
    if (cachedZoneId != null) {
      return cachedZoneId;
    }
    PGSetting setting = internalGet(toKey(null, "timezone"), false);
    if (setting == null) {
      return ZoneId.systemDefault();
    }
    String id =
        tryGetFirstNonNull(
            ZoneId.systemDefault().getId(),
            setting::getSetting,
            setting::getResetVal,
            setting::getBootVal);

    return (cachedZoneId = zoneIdFromString(id));
  }

  private ZoneId zoneIdFromString(String value) {
    try {
      return ZoneId.of(value);
    } catch (Throwable ignore) {
      return ZoneId.systemDefault();
    }
  }

  @SafeVarargs
  static <T> T tryGetFirstNonNull(T defaultResult, Callable<T>... callables) {
    T value;
    for (Callable<T> callable : callables) {
      if ((value = tryGet(callable)) != null) {
        return value;
      }
    }
    return defaultResult;
  }

  static <T> T tryGet(Callable<T> callable) {
    try {
      return callable.call();
    } catch (Throwable ignored) {
      return null;
    }
  }
}
