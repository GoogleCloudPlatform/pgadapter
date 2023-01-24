// Copyright 2020 Google LLC
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
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.myadapter.command.commands.QueryMessageProcessor;
import com.google.cloud.spanner.myadapter.parsers.BooleanParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/** {@link SessionState} contains all session variables for a connection. */
@InternalApi
public class SessionState {

  public static final String ONE = "1";
  public static final String ZERO = "0";
  public static final ImmutableList<String> SET_NAMES_CHARASETS =
      ImmutableList.of("character_set_client", "character_set_connection", "character_set_results");
  public static final String AUTOCOMMIT_KEYWORD = "autocommit";
  private volatile ProtocolStatus protocolStatus;

  static final Map<String, SystemVariable> DEFAULT_SETTINGS = new HashMap<>();
  private static final Logger logger = Logger.getLogger(QueryMessageProcessor.class.getName());
  private static String NAMES = "names";

  public enum SessionVariableType {
    SYSTEM,
    USER_DEFINED
  }

  static {
    for (SystemVariable setting : SystemVariable.read()) {
      DEFAULT_SETTINGS.put(setting.getName().toLowerCase(Locale.ROOT), setting);
    }
  }

  private final Map<String, SystemVariable> settings;

  public SessionState() {
    this(ImmutableMap.of());
  }

  @VisibleForTesting
  SessionState(Map<String, SystemVariable> extraServerSettings) {
    this.protocolStatus = ProtocolStatus.CONNECTION_INITIATED;

    Preconditions.checkNotNull(extraServerSettings);
    this.settings = new HashMap<>(DEFAULT_SETTINGS.size() + extraServerSettings.size());
    for (Entry<String, SystemVariable> entry : DEFAULT_SETTINGS.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
    for (Entry<String, SystemVariable> entry : extraServerSettings.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
  }

  public ProtocolStatus getProtocolStatus() {
    return protocolStatus;
  }

  public void setProtocolStatus(ProtocolStatus protocolStatus) {
    this.protocolStatus = protocolStatus;
  }

  Map<String, SystemVariable> getVariableMapForType(SessionVariableType scope) {
    switch (scope) {
      case SYSTEM:
        return settings;
      default:
        throw unknownParamError("scope");
    }
  }
  /**
   * Sets the value of the specified setting. The new value will be persisted if the current
   * transaction is committed. The value will be lost if the transaction is rolled back.
   */
  public void set(String name, String value, SessionVariableType scope) {
    logger.log(
        Level.INFO,
        () -> String.format("Setting system variable %s to %s at scope %s", name, value, scope));
    Map<String, SystemVariable> variableMap = getVariableMapForType(scope);
    internalSet(name.toLowerCase(Locale.ROOT), value, variableMap);
  }

  private void internalSet(String name, String value, Map<String, SystemVariable> variableMap) {
    if (NAMES.equals(name)) {
      // TODO: Consider handling "SET NAMES" as a separate statement type.
      handleNames(value);
      return;
    }
    if (AUTOCOMMIT_KEYWORD.equals(name)) {
      // Autocommit value needs to be converted to an integer as internally autocommit is being
      // tracked as integer.
      value = inferAutocommitValue(value);
    }
    SystemVariable variable = variableMap.get(name);
    if (variable == null) {
      throw unknownVariableError(name);
    }
    variable.setValue(value);
  }

  private String inferAutocommitValue(String value) {
    if (BooleanParser.TRUE_VALUES.contains(value)) {
      return ONE;
    }
    if (BooleanParser.FALSE_VALUES.contains(value)) {
      return ZERO;
    }
    throw invalidValueError("autocommit", value);
  }

  private void handleNames(String value) {
    logger.log(Level.INFO, () -> String.format("Setting all character sets to %s", value));
    Map<String, SystemVariable> variableMap = getVariableMapForType(SessionVariableType.SYSTEM);
    for (String charset : SET_NAMES_CHARASETS) {
      SystemVariable variable = variableMap.get(charset);
      Preconditions.checkNotNull(variable);
      variable.setValue(value);
    }
  }

  /** Returns the current value of the specified setting. */
  public SystemVariable get(String name, SessionVariableType scope) {
    Map<String, SystemVariable> variableMap = getVariableMapForType(scope);
    return internalGet(name.toLowerCase(Locale.ROOT), variableMap);
  }

  private SystemVariable internalGet(String key, Map<String, SystemVariable> variableMap) {
    SystemVariable variable = variableMap.get(key);
    if (variable == null) {
      throw unknownVariableError(key);
    }
    return variable;
  }

  static SpannerException invalidValueError(String key, String value) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("Variable \"%s\" can't be set to the value of ", key, value));
  }

  static SpannerException unknownParamError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("unrecognized configuration parameter \"%s\"", key));
  }

  static SpannerException unknownVariableError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT, String.format("Unknown system variable '%s'", key));
  }
}
