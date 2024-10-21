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

import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.error.PGException;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Represents a row in the pg_settings table. */
@InternalApi
public class PGSetting {
  public enum Context {
    USER,
    SUPERUSER,
    BACKEND,
    SUPERUSER_BACKEND,
    SIGHUP,
    POSTMASTER,
    INTERNAL;

    String getSqlValue() {
      return name().toLowerCase().replace('_', '-');
    }
  }

  private static final int NAME_INDEX = 0;
  private static final int SETTING_INDEX = 1;
  private static final int UNIT_INDEX = 2;
  private static final int CATEGORY_INDEX = 3;
  private static final int SHORT_DESC_INDEX = 4;
  private static final int EXTRA_DESC_INDEX = 5;
  private static final int CONTEXT_INDEX = 6;
  private static final int VARTYPE_INDEX = 7;
  private static final int SOURCE_INDEX = 8;
  private static final int MIN_VAL_INDEX = 9;
  private static final int MAX_VAL_INDEX = 10;
  private static final int ENUM_VALS_INDEX = 11;
  private static final int BOOT_VAL_INDEX = 12;
  private static final int RESET_VAL_INDEX = 13;
  private static final int SOURCEFILE_INDEX = 14;
  private static final int SOURCELINE_INDEX = 15;
  private static final int PENDING_RESTART_INDEX = 16;
  /** The column names of the pg_settings table. */
  private static final ImmutableList<String> COLUMN_NAMES =
      ImmutableList.of(
          "name",
          "setting",
          "unit",
          "category",
          "short_desc",
          "extra_desc",
          "context",
          "vartype",
          "source",
          "min_val",
          "max_val",
          "enumvals",
          "boot_val",
          "reset_val",
          "sourcefile",
          "sourceline",
          "pending_restart");

  private final String extension;
  private final String name;
  private final String category;
  private final String shortDesc;
  private final String extraDesc;
  private final Context context;
  private final String vartype;
  private final String minVal;
  private final String maxVal;
  private final String[] enumVals;
  private final ImmutableList<String> upperCaseEnumVals;

  private String setting;
  private String unit;
  private String source;
  private String bootVal;
  private String resetVal;
  private String sourcefile;
  private Integer sourceline;
  private boolean pendingRestart;

  static ImmutableList<PGSetting> read() {
    ImmutableList.Builder<PGSetting> builder = ImmutableList.builder();
    try (Scanner scanner =
        new Scanner(
            Objects.requireNonNull(PGSetting.class.getResourceAsStream("pg_settings.txt")))) {
      while (scanner.hasNextLine()) {
        builder.add(parse(scanner.nextLine()));
      }
    }
    return builder.build();
  }

  static @Nonnull PGSetting parse(String line) {
    String[] values = line.split("\t");
    Preconditions.checkArgument(values.length == 17);
    return new PGSetting(
        /* extension = */ null,
        parseString(values[NAME_INDEX]),
        parseString(values[CATEGORY_INDEX]),
        parseString(values[SHORT_DESC_INDEX]),
        parseString(values[EXTRA_DESC_INDEX]),
        parseString(values[CONTEXT_INDEX]),
        parseString(values[VARTYPE_INDEX]),
        parseString(values[MIN_VAL_INDEX]),
        parseString(values[MAX_VAL_INDEX]),
        parseStringArray(values[ENUM_VALS_INDEX]),
        parseString(values[SETTING_INDEX]),
        parseString(values[UNIT_INDEX]),
        parseString(values[SOURCE_INDEX]),
        parseString(values[BOOT_VAL_INDEX]),
        parseString(values[RESET_VAL_INDEX]),
        parseString(values[SOURCEFILE_INDEX]),
        parseInteger(values[SOURCELINE_INDEX]),
        values[PENDING_RESTART_INDEX].equals("t"));
  }

  static String parseString(String value) {
    if ("\\N".equals(value)) {
      return null;
    }
    return value;
  }

  static String[] parseStringArray(String value) {
    if ("\\N".equals(value)) {
      return null;
    }
    Preconditions.checkArgument(value.startsWith("{") && value.endsWith("}"));
    return value.substring(1, value.length() - 1).split(",");
  }

  static Integer parseInteger(String value) {
    if ("\\N".equals(value)) {
      return null;
    }
    return Integer.valueOf(value);
  }

  PGSetting(String extension, String name) {
    this.extension = extension;
    this.name = name;
    this.context = Context.USER;
    this.category = null;
    this.shortDesc = null;
    this.extraDesc = null;
    this.vartype = null;
    this.minVal = null;
    this.maxVal = null;
    this.enumVals = null;
    this.upperCaseEnumVals = null;
  }

  @VisibleForTesting
  PGSetting(
      String extension,
      String name,
      String category,
      String shortDesc,
      String extraDesc,
      String context,
      String vartype,
      String minVal,
      String maxVal,
      String[] enumVals,
      String setting,
      String unit,
      String source,
      String bootVal,
      String resetVal,
      String sourcefile,
      Integer sourceline,
      boolean pendingRestart) {
    this.extension = extension;
    this.name = name;
    this.category = category;
    this.shortDesc = shortDesc;
    this.extraDesc = extraDesc;
    this.context = tryParseContext(context);
    this.vartype = vartype;
    this.minVal = minVal;
    this.maxVal = maxVal;
    this.enumVals = enumVals;
    this.upperCaseEnumVals =
        enumVals == null
            ? null
            : ImmutableList.copyOf(
                Arrays.stream(enumVals)
                    .map(v -> v.toUpperCase(Locale.ENGLISH))
                    .collect(Collectors.toList()));
    this.setting = setting;
    this.unit = unit;
    this.source = source;
    this.bootVal = bootVal;
    this.resetVal = resetVal;
    this.sourcefile = sourcefile;
    this.sourceline = sourceline;
    this.pendingRestart = pendingRestart;
  }

  /** Returns a copy of this {@link PGSetting}. */
  PGSetting copy() {
    return new PGSetting(
        extension,
        name,
        category,
        shortDesc,
        extraDesc,
        context.name(),
        vartype,
        minVal,
        maxVal,
        enumVals,
        setting,
        unit,
        source,
        bootVal,
        resetVal,
        sourcefile,
        sourceline,
        pendingRestart);
  }

  static Context tryParseContext(String value) {
    try {
      return Strings.isNullOrEmpty(value)
          ? Context.USER
          : Context.valueOf(value.toUpperCase().replace('-', '_'));
    } catch (Exception ignore) {
      return Context.USER;
    }
  }

  /**
   * Returns a SELECT statement that can be used in a query or CTE to select jsonb-generated
   * settings.
   */
  static String getSelectJsonbSetting() {
    return "select "
        + "t->>'name' as name, "
        + "t->>'setting' as setting, "
        + "t->>'unit' as unit, "
        + "t->>'category' as category, "
        + "t->>'short_desc' as short_desc, "
        + "t->>'extra_desc' as extra_desc, "
        + "t->>'context' as context, "
        + "t->>'vartype' as vartype, "
        + "t->>'min_val' as min_val, "
        + "t->>'max_val' as max_val, "
        + "case when t->>'enumvals' is null then null::text[] else spanner.string_array((t->>'enumvals')::jsonb) end as enumvals, "
        + "t->>'boot_val' as boot_val, "
        + "t->>'reset_val' as reset_val, "
        + "t->>'source' as source, "
        + "(t->>'sourcefile')::varchar as sourcefile, "
        + "(t->>'sourceline')::bigint as sourceline, "
        + "(t->>'pending_restart')::boolean as pending_restart\n";
  }

  /** Returns this setting as a jsonb literal expression that can be used in a query or CTE. */
  String getJsonbLiteral() {
    return "'{"
        + toJsonbElement("name", getCasePreservingKey())
        + ","
        + toJsonbElement("setting", setting)
        + ","
        + toJsonbElement("unit", unit)
        + ","
        + toJsonbElement("category", category)
        + ","
        + toJsonbElement("short_desc", (String) null)
        + ","
        + toJsonbElement("extra_desc", (String) null)
        + ","
        + toJsonbElement("context", context.getSqlValue())
        + ","
        + toJsonbElement("vartype", vartype)
        + ","
        + toJsonbElement("min_val", minVal)
        + ","
        + toJsonbElement("max_val", maxVal)
        + ","
        + toJsonbElement("enum_vals", enumVals)
        + ","
        + toJsonbElement("boot_val", bootVal)
        + ","
        + toJsonbElement("reset_val", resetVal)
        + ","
        + toJsonbElement("source", source)
        + ","
        + toJsonbElement("sourcefile", (String) null)
        + ","
        + toJsonbElement("sourceline", (Integer) null)
        + ","
        + toJsonbElement("pending_restart", pendingRestart)
        + "}'::jsonb";
  }

  /** Returns the column names of the pg_settings table. */
  static ImmutableList<String> getColumnNames() {
    return COLUMN_NAMES;
  }

  String toJsonbElement(String name, String value) {
    return "\"" + name + "\":" + toJsonbExpression(value);
  }

  String toJsonbElement(String name, Integer value) {
    return "\"" + name + "\":" + toJsonbExpression(value);
  }

  String toJsonbElement(String name, Boolean value) {
    return "\"" + name + "\":" + toJsonbExpression(value);
  }

  String toJsonbElement(String name, String[] value) {
    return "\"" + name + "\":" + toJsonbExpression(value);
  }

  String toJsonbExpression(String value) {
    return value == null ? "null" : "\"" + value + "\"";
  }

  String toJsonbExpression(Integer value) {
    return value == null ? "null" : value.toString();
  }

  String toJsonbExpression(Boolean value) {
    return value == null ? "null" : value.toString();
  }

  String toJsonbExpression(String[] value) {
    return value == null
        ? "null"
        : "["
            + Arrays.stream(value)
                .map(s -> s.startsWith("\"") ? s : "\"" + s + "\"")
                .collect(Collectors.joining(", "))
            + "]";
  }

  /**
   * Returns the case-preserving key of this setting. Some settings have a key that is written in
   * camel case (e.g. 'DateStyle') instead of snake case (e.g. 'server_version'). This key should
   * not be used to look up a setting in the session state map, but should be used in for example
   * the pg_settings table.
   */
  public String getCasePreservingKey() {
    if (extension == null) {
      return name;
    }
    return extension + "." + name;
  }

  public String getExtension() {
    return extension;
  }

  public String getName() {
    return name;
  }

  /** Returns the value of this setting. */
  public String getSetting() {
    return setting;
  }

  /** Initializes the value of the setting without checking for validity. */
  void initSettingValue(String value) {
    this.setting = value;
  }

  /** Initializes the value of the setting at connection startup. */
  void initConnectionValue(String value) {
    setSetting(Context.BACKEND, value);
    this.resetVal = value;
  }

  /**
   * Sets the value for this setting. Throws {@link SpannerException} if the value is not valid, or
   * if the setting is not settable.
   */
  void setSetting(Context context, String value) {
    if (this.context != null) {
      checkValidContext(context);
    }
    if (this.vartype != null) {
      // Check validity of the value.
      value = checkValidValue(value);
    }
    this.setting = value;
  }

  boolean isSettable(Context context) {
    Preconditions.checkNotNull(context);
    return this.context.ordinal() <= context.ordinal();
  }

  private void checkValidContext(Context context) {
    if (!isSettable(context)) {
      throw invalidContextError(getCasePreservingKey(), this.context);
    }
  }

  static SpannerException invalidContextError(String key, Context context) {
    switch (context) {
      case INTERNAL:
        return SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT, String.format("parameter \"%s\" cannot be changed", key));
      case POSTMASTER:
        return SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("parameter \"%s\" cannot be changed without restarting the server", key));
      case SIGHUP:
        return SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("parameter \"%s\" cannot be changed now", key));
      case SUPERUSER_BACKEND:
      case BACKEND:
      case SUPERUSER:
      case USER:
      default:
        return SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("parameter \"%s\" cannot be set after connection start", key));
    }
  }

  private String checkValidValue(String value) {
    if ("bool".equals(this.vartype)) {
      // Just verify that it is a valid boolean. This will throw an IllegalArgumentException if
      // setting is not a valid boolean value.
      try {
        BooleanParser.toBoolean(value);
      } catch (PGException exception) {
        throw invalidBoolError(getCasePreservingKey());
      }
    } else if ("integer".equals(this.vartype)) {
      try {
        Integer.parseInt(value);
      } catch (NumberFormatException exception) {
        throw invalidValueError(getCasePreservingKey(), value);
      }
    } else if ("real".equals(this.vartype)) {
      try {
        Double.parseDouble(value);
      } catch (NumberFormatException exception) {
        throw invalidValueError(getCasePreservingKey(), value);
      }
    } else if (upperCaseEnumVals != null
        && !upperCaseEnumVals.contains(
            MoreObjects.firstNonNull(value, "").toUpperCase(Locale.ENGLISH))) {
      throw invalidEnumError(getCasePreservingKey(), value, enumVals);
    } else if ("TimeZone".equals(this.name)) {
      try {
        value = convertToValidZoneId(value);
        ZoneId.of(value);
      } catch (Exception ignore) {
        throw invalidValueError(this.name, value);
      }
    }
    return value;
  }

  static String convertToValidZoneId(String value) {
    if ("utc".equalsIgnoreCase(value)) {
      return "UTC";
    }
    if ("localtime".equalsIgnoreCase(value)) {
      return ZoneId.systemDefault().getId();
    }
    for (String zoneId : ZoneId.getAvailableZoneIds()) {
      if (zoneId.equalsIgnoreCase(value)) {
        return zoneId;
      }
    }
    for (Map.Entry<String, String> shortId : ZoneId.SHORT_IDS.entrySet()) {
      if (shortId.getKey().equalsIgnoreCase(value)) {
        return shortId.getValue();
      }
    }
    return value;
  }

  static SpannerException invalidBoolError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("parameter \"%s\" requires a Boolean value", key));
  }

  static SpannerException invalidValueError(String key, String value) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("invalid value for parameter \"%s\": \"%s\"", key, value));
  }

  static PGException invalidEnumError(String key, String value, String[] enumVals) {
    return PGException.newBuilder(
            String.format("invalid value for parameter \"%s\": \"%s\"", key, value))
        .setHints(String.format("Available values: %s.", String.join(", ", enumVals)))
        .build();
  }

  public String getCategory() {
    return category;
  }

  public String getShortDesc() {
    return shortDesc;
  }

  public String getExtraDesc() {
    return extraDesc;
  }

  public Context getContext() {
    return context;
  }

  public String getVartype() {
    return vartype;
  }

  public String getMinVal() {
    return minVal;
  }

  public String getMaxVal() {
    return maxVal;
  }

  public String[] getEnumVals() {
    return enumVals;
  }

  public String getResetVal() {
    return resetVal;
  }

  public String getBootVal() {
    return bootVal;
  }
}
