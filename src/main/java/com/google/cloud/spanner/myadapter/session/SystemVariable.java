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

package com.google.cloud.spanner.myadapter.session;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.myadapter.utils.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.*;
import javax.annotation.Nonnull;

/** Represents a row in the pg_settings table. */
@InternalApi
public class SystemVariable {
  private static final int NAME_INDEX = 0;
  private static final int DATATYPE_INDEX = 1;
  private static final int VALUE_INDEX = 2;

  private String name;
  private Type spannerType;
  private String value;

  static ImmutableList<SystemVariable> read() {
    ImmutableList.Builder<SystemVariable> builder = ImmutableList.builder();
    try (Scanner scanner =
        new Scanner(
            Objects.requireNonNull(
                SystemVariable.class.getResourceAsStream("system_variables.txt")))) {
      while (scanner.hasNextLine()) {
        builder.add(parse(scanner.nextLine()));
      }
    }
    return builder.build();
  }

  static @Nonnull SystemVariable parse(String line) {
    String[] values = line.split("\t");
    Preconditions.checkArgument(values.length == 3);
    return new SystemVariable(
        parseString(values[NAME_INDEX]),
        Converter.typeCodeToSpannerType(parseString(values[DATATYPE_INDEX])),
        parseString(values[VALUE_INDEX]));
  }

  static String parseString(String value) {
    if ("\\N".equals(value)) {
      return "";
    }
    return value;
  }

  SystemVariable(String name, Type variableType, String value) {
    this.name = name;
    this.spannerType = variableType;
    this.value = value;
  }

  /** Returns a copy of this {@link SystemVariable}. */
  SystemVariable copy() {
    return new SystemVariable(name, spannerType, value);
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return spannerType;
  }

  /** Returns the value of this setting. */
  public String getValue() {
    return value;
  }

  /**
   * Sets the value for this setting. Throws {@link SpannerException} if the value is not valid, or
   * if the setting is not settable.
   */
  void setValue(String value) {
    if (this.spannerType != null) {
      // Check validity of the value.
      value = checkValidValue(value);
    }
    this.value = value;
  }

  private String checkValidValue(String value) {
    // TODO implement this.
    return value;
  }
}
