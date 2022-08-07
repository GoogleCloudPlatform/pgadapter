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
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** {@link SessionState} contains all session variables for a connection. */
@InternalApi
public class SessionState {
  private static final Map<String, PGSetting> SERVER_SETTINGS = new HashMap<>();

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
    this.settings = new HashMap<>(SERVER_SETTINGS);
    this.settings.get("server_version").initSettingValue(options.getServerVersion());
  }

  private static String toKey(String extension, String name) {
    return extension == null
        ? name.toLowerCase(Locale.ROOT)
        : extension.toLowerCase(Locale.ROOT) + "." + name.toLowerCase(Locale.ROOT);
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
    newSetting.setSetting(setting);
    currentSettings.put(key, newSetting);
  }

  /** Returns the current value of the specified setting. */
  public PGSetting get(String extension, String name) {
    return internalGet(toKey(extension, name));
  }

  private PGSetting internalGet(String key) {
    if (localSettings != null && localSettings.containsKey(key)) {
      return localSettings.get(key);
    }
    if (transactionSettings != null && transactionSettings.containsKey(key)) {
      return transactionSettings.get(key);
    }
    if (settings.containsKey(key)) {
      return settings.get(key);
    }
    throw unknownParamError(key);
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
      result.add(internalGet(key));
    }
    result.sort(Comparator.comparing(PGSetting::getKey));
    return result;
  }

  /** Resets all values to their 'reset' value. */
  public void resetAll() {
    for (PGSetting setting : getAll()) {
      if (setting.isSettable() && !Objects.equals(setting.getSetting(), setting.getResetVal())) {
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
}
