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

import java.util.HashMap;
import java.util.Map;

public class SessionState {
  private final Map<String, PGSetting> settings = new HashMap<>();

  /** transactionSettings are the modified session settings during a transaction. */
  private Map<String, PGSetting> transactionSettings;
  /** localSettings are the modified local settings during a transaction. */
  private Map<String, PGSetting> localSettings;

  private boolean inTransaction;

  public SessionState() {
    for (PGSetting setting : PGSetting.read()) {
      settings.put(setting.getName(), setting);
    }
  }

  public void set(String name, String setting) {
    if (transactionSettings == null) {
      transactionSettings = new HashMap<>();
    }
    internalSet(name, setting, transactionSettings);
    localSettings.remove(name);
  }

  public void setLocal(String name, String setting) {
    if (localSettings == null) {
      localSettings = new HashMap<>();
    }
    internalSet(name, setting, localSettings);
  }

  private void internalSet(String name, String setting, Map<String, PGSetting> currentSettings) {
    PGSetting newSetting = currentSettings.get(name);
    if (newSetting == null) {
      PGSetting existingSetting = settings.get(name);
      if (existingSetting == null) {
        // TODO: Check if it is an extension setting, otherwise return error.
        newSetting = new PGSetting(name);
      } else {
        newSetting = existingSetting.copy();
      }
    }
    newSetting.setSetting(setting);
    currentSettings.put(name, newSetting);
  }

  /**
   * Begin a transaction. Any changes to the session state during this transaction will be persisted
   * if the transaction is committed, or abandoned if the transaction is rolled back.
   */
  public void begin() {
    this.inTransaction = true;
  }

  /**
   * Commits the current transaction and persists any changes to the settings (except local
   * changes).
   */
  public void commit() {
    this.inTransaction = false;
  }

  /** Rolls back the current transaction and abandons any pending changes to the settings. */
  public void rollback() {
    this.inTransaction = false;
  }
}
