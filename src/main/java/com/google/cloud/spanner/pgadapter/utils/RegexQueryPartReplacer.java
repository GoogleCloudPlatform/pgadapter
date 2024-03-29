// Copyright 2023 Google LLC
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

import com.google.cloud.Tuple;
import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link QueryPartReplacer} that replaces a regex {@link java.util.regex.Pattern} with a fixed
 * string.
 */
public class RegexQueryPartReplacer implements QueryPartReplacer {
  private final Pattern pattern;
  private final Supplier<String> replacement;
  private final ReplacementStatus replacementStatus;
  private final boolean replaceEntireStatement;

  /** Replace all occurrences of the given pattern with the given static replacement. */
  public static RegexQueryPartReplacer replace(Pattern pattern, String replacement) {
    return new RegexQueryPartReplacer(
        pattern, Suppliers.ofInstance(replacement), ReplacementStatus.CONTINUE, false);
  }

  /**
   * Replace all occurrences of the given pattern with the value that is returned by the given
   * {@link Supplier} function.
   */
  public static RegexQueryPartReplacer replace(Pattern pattern, Supplier<String> replacement) {
    return new RegexQueryPartReplacer(pattern, replacement, ReplacementStatus.CONTINUE, false);
  }

  /**
   * Replace the first occurrence of the given pattern with the given static value and then stop.
   */
  public static RegexQueryPartReplacer replaceAndStop(Pattern pattern, String replacement) {
    return new RegexQueryPartReplacer(
        pattern, Suppliers.ofInstance(replacement), ReplacementStatus.STOP, false);
  }

  /**
   * Replace the first occurrence of the given pattern with the value that is returned by the given
   * {@link Supplier} function and then stop.
   */
  public static RegexQueryPartReplacer replaceAndStop(
      Pattern pattern, Supplier<String> replacement) {
    return new RegexQueryPartReplacer(pattern, replacement, ReplacementStatus.STOP, false);
  }
  /** Replace the entire with the given static value and then stop. */
  public static RegexQueryPartReplacer replaceAllAndStop(Pattern pattern, String replacement) {
    return new RegexQueryPartReplacer(
        pattern, Suppliers.ofInstance(replacement), ReplacementStatus.STOP, true);
  }

  private RegexQueryPartReplacer(
      Pattern pattern,
      Supplier<String> replacement,
      ReplacementStatus replacementStatus,
      boolean replaceEntireStatement) {
    this.pattern = pattern;
    this.replacement = replacement;
    this.replacementStatus = replacementStatus;
    this.replaceEntireStatement = replaceEntireStatement;
  }

  @Override
  public Tuple<String, ReplacementStatus> replace(String sql) {
    Matcher matcher = pattern.matcher(sql);
    if (matcher.find()) {
      if (replaceEntireStatement) {
        return Tuple.of(replacement.get(), replacementStatus);
      }
      return Tuple.of(matcher.replaceAll(replacement.get()), replacementStatus);
    }
    return Tuple.of(sql, ReplacementStatus.CONTINUE);
  }
}
