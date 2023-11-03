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

public class Logging {
  public enum Action {
    Starting,
    Finished,
  }

  public static Supplier<String> format(String method, Supplier<String> message) {
    return () ->
        String.format("[%s]: [%s] " + message.get(), Thread.currentThread().getName(), method);
  }

  public static Supplier<String> format(String method, Action action) {
    return () -> String.format("[%s]: [%s] [%s]", Thread.currentThread().getName(), method, action);
  }

  public static Supplier<String> format(String method, Action action, Supplier<String> message) {
    return () ->
        String.format(
            "[%s]: [%s] [%s] " + message.get(), Thread.currentThread().getName(), method, action);
  }
}
