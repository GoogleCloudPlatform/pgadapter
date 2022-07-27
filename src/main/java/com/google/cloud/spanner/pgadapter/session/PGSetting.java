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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Objects;
import java.util.Scanner;
import javax.annotation.Nonnull;

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
    return ImmutableList.of();
  }

  static @Nonnull PGSetting parse(String line) {
    String[] values = line.split("\t");
    Preconditions.checkArgument(values.length == 17);
    return new PGSetting(
        values[NAME_INDEX],
        values[CATEGORY_INDEX],
        values[SHORT_DESC_INDEX],
        values[EXTRA_DESC_INDEX],
        values[CONTEXT_INDEX],
        values[VARTYPE_INDEX],
        values[MIN_VAL_INDEX],
        values[MAX_VAL_INDEX],
        parseStringArray(values[ENUM_VALS_INDEX]),
        values[SETTING_INDEX],
        values[UNIT_INDEX],
        values[SOURCE_INDEX],
        values[BOOT_VAL_INDEX],
        values[RESET_VAL_INDEX],
        values[SOURCEFILE_INDEX],
        parseInteger(values[SOURCELINE_INDEX]),
        values[PENDING_RESTART_INDEX].equals("t"));
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

  PGSetting(String name) {
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

  public String getName() {
    return name;
  }

  public String getSetting() {
    return setting;
  }

  public void setSetting(String setting) {
    this.setting = setting;
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
}
