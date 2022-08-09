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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.pgadapter.parsers.BooleanParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Objects;
import java.util.Scanner;
import javax.annotation.Nonnull;

/** Represents a row in the pg_settings table. */
@InternalApi
public class PGSetting {
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

  private final String extension;
  private final String name;
  private final String category;
  private final String shortDesc;
  private final String extraDesc;
  private final String context;
  private final String vartype;
  private final String minVal;
  private final String maxVal;
  private final String[] enumVals;

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
    this.category = null;
    this.shortDesc = null;
    this.extraDesc = null;
    this.context = null;
    this.vartype = null;
    this.minVal = null;
    this.maxVal = null;
    this.enumVals = null;
  }

  private PGSetting(
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
    this.context = context;
    this.vartype = vartype;
    this.minVal = minVal;
    this.maxVal = maxVal;
    this.enumVals = enumVals;
    this.setting = setting;
    this.unit = unit;
    this.source = source;
    this.bootVal = bootVal;
    this.resetVal = resetVal;
    this.sourcefile = sourcefile;
    this.sourceline = sourceline;
    this.pendingRestart = pendingRestart;
  }

  PGSetting copy() {
    return new PGSetting(
        extension,
        name,
        category,
        shortDesc,
        extraDesc,
        context,
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

  public String getKey() {
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

  /**
   * Sets the value for this setting. Throws {@link SpannerException} if the value is not valid, or
   * if the setting is not settable.
   */
  void setSetting(String value) {
    if (this.context != null) {
      checkValidContext();
    }
    if (this.vartype != null) {
      // Check validity of the value.
      checkValidValue(value);
    }
    this.setting = value;
  }

  boolean isSettable() {
    // Consider all users as superuser.
    return "user".equals(this.context) || "superuser".equals(this.context);
  }

  private void checkValidContext() {
    if (!isSettable()) {
      throw invalidContextError(getKey(), this.context);
    }
  }

  static SpannerException invalidContextError(String key, String context) {
    if ("internal".equals(context)) {
      return SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, String.format("parameter \"%s\" cannot be changed", key));
    }
    if ("postmaster".equals(context)) {
      return SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          String.format("parameter \"%s\" cannot be changed without restarting the server", key));
    }
    if ("sighup".equals(context)) {
      return SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT, String.format("parameter \"%s\" cannot be changed now", key));
    }
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("parameter \"%s\" cannot be set after connection start", key));
  }

  private void checkValidValue(String value) {
    if ("bool".equals(this.vartype)) {
      // Just verify that it is a valid boolean. This will throw an IllegalArgumentException if
      // setting is not a valid boolean value.
      try {
        BooleanParser.toBoolean(value);
      } catch (IllegalArgumentException exception) {
        throw invalidBoolError(getKey());
      }
    } else if ("integer".equals(this.vartype)) {
      try {
        Integer.parseInt(value);
      } catch (NumberFormatException exception) {
        throw invalidValueError(getKey(), value);
      }
    } else if ("real".equals(this.vartype)) {
      try {
        Double.parseDouble(value);
      } catch (NumberFormatException exception) {
        throw invalidValueError(getKey(), value);
      }
    } else if (enumVals != null && !Iterables.contains(Arrays.asList(this.enumVals), value)) {
      throw invalidValueError(getKey(), value);
    }
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

  public String getCategory() {
    return category;
  }

  public String getShortDesc() {
    return shortDesc;
  }

  public String getExtraDesc() {
    return extraDesc;
  }

  public String getContext() {
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
}
