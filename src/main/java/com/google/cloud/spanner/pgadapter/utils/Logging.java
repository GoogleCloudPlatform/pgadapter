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

import java.util.function.Supplier;

/** Util class for formatting log messages. */
public class Logging {
  public enum Action {
    Starting,
    Finished,
  }

  /** Format a log message by prepending the current thread name and method to the message. */
  public static Supplier<String> format(String method, Supplier<String> message) {
    return () ->
        String.format("[%s]: [%s] " + message.get(), Thread.currentThread().getName(), method);
  }

  /** Create a log message with the current thread name, method and action. */
  public static Supplier<String> format(String method, Action action) {
    return () -> String.format("[%s]: [%s] [%s]", Thread.currentThread().getName(), method, action);
  }

  /**
   * Format a log message by prepending the current thread name, method, and action to the message.
   */
  public static Supplier<String> format(String method, Action action, Supplier<String> message) {
    return () ->
        String.format(
            "[%s]: [%s] [%s] " + message.get(), Thread.currentThread().getName(), method, action);
  }
}
